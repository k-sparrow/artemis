load("@aspect_bazel_lib//lib:tar.bzl", "mtree_mutate", "mtree_spec", "tar")
load("@aspect_bazel_lib//lib:testing.bzl", "assert_archive_contains")
load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@rules_oci//oci:defs.bzl", "oci_image")

def kafka_connect_image(*, name, base, connectors_srcs, cp_components_dir, **kwargs):
    """Creates a custom Kafka Connect OCI image

    Adds packages for custom Kafka Connect connectors, SMT's and
    various other plugins under a unified directory

    Args:
      name: name of the target OCI image
            Every othe sub-target name will be derived from this
      base: the base image on which we add connector plugins packages
      connectors_srcs: Sources for each pluging package, usually in the
                       form of a group of at least one JAR file (but not only)
                       These will be packaged into a list of tar archives
                       and added as layers to the resulting OCI image
      cp_components_dir: the shared directory under which Kafka Connect
                         process expects to find loadable JAR archives
                         This is where the plugins' tar archives
                         will be moved to inside the resulting OCI image
                         file system
      **kwargs: misc key-value
    """
    for connector_name, connector_srcs in connectors_srcs.items():
        # create a manifest file for each of the connector original sources
        mtree_spec(
            name = "{connector}.mtree".format(connector = connector_name),
            srcs = connector_srcs,
        )

        # change the root path of the sources for each connector
        #
        # The root path in each manifest file should now point to
        # /usr/share/confluent-hub-components/...
        #
        # where confluent's Kafka Connect worker will expect to
        # find executable JAR files of the connector
        mtree_mutate(
            name = "cp-{connector}.mtree".format(connector = connector_name),
            mtree = ":{connector}.mtree".format(connector = connector_name),
            # package_dir = "usr/share/confluent-hub-components",
            package_dir = cp_components_dir,
        )

        # package the modified tar's srcs as layers for the cp Kafka Connect
        # image
        tar(
            name = "cp-{connector}".format(connector = connector_name),
            srcs = connector_srcs,
            args = [
                "--exclude=*.bazel",
                "--exclude=WORKSPACE",
            ],
            mtree = ":cp-{connector}.mtree".format(connector = connector_name),
        )

    # package the modified tar's srcs as layers for the cp Kafka Connect
    # image
    # All packaged connectors should now sit under /usr/share/confluent-hub-components/
    # which is where CP's Kafka Connect worker process expects them to be
    #
    # Information about them will be loaded at start time of the container
    # and they will be readily available for instantiation via Kafka Connect's
    # REST API (at port 8083 by default) once the worker is ready and healthy
    oci_image(
        name = name,
        base = base,
        # add the connectors to the image as tar layers
        # they should eventually sit under /usr/share/conluent-hub-components
        tars = [
            ":cp-{connector}".format(connector = connector_name)
            for connector_name in connectors_srcs.keys()
        ] + kwargs.pop("tars", []),
        **kwargs
    )

def kafka_connect_connector_instance(name, connector_name, connector_config, out = None):
    """
    Create a JSON file from a connector name and its configuration

    The general structure of the output JSON file should look like:
    ```json
    {
        "name": <connector_name>,
        "config": <config>
    }
    ```

    This is how Kafka Connect expects configuration to be sent via its REST API
    """
    if not out:
        out = "{name}.json".format(name = name)

    write_file(
        name = name,
        out = out,
        content = [
            json.encode(
                {
                    "name": connector_name,
                    "config": connector_config,
                },
            ),
        ],
        is_executable = False,
    )

def kafka_connect_connectors_pkg(name, connectors, package_dir):
    # create a manifest file from the connector json files specified
    # by the list of json file targets named 'connectors'
    # this is done in order to allow manifest mutation,
    # which evetually move the configuration files under
    # the 'package_dir' directory
    mtree_spec(
        name = "{name}.mtree".format(name = name),
        srcs = connectors,
    )

    # mutate the manifest files to move connector srcs
    # to the package directory
    mtree_mutate(
        name = "{name}.mut".format(name = name),
        mtree = ":{name}.mtree".format(name = name),
        package_dir = package_dir,
        strip_prefix = native.package_name(),
    )

    # package the connector JSON files under /<package_dir>
    tar(
        name = name,
        srcs = connectors,
        mtree = ":{name}.mut".format(name = name),
        args = [
            "--exclude=*.bazel",
            "--exclude=WORKSPACE",
        ],
    )

    # add a safety assertion for the archive structure to check we
    # didn't screw up
    assert_archive_contains(
        name = "{name}-archive-structure-test".format(name = name),
        archive = ":{name}".format(name = name),
        type = "tar",
        expected = [
            "/{package_dir}/{connector}.json".format(
                package_dir = package_dir,
                # connector is actually a target name, not the json file name
                connector = connector.split(":")[-1],
            )
            for connector in connectors
        ],
    )

# Initialization script for tar packaging
# Note: the script is parameterized and expects KAFKA_CONNECT_CLUSTER_URI
# to be supplied via cmdline or via docker compose environment variables
#
# TODO: consider using Bazel's expand_template instead of this
_KAFKA_CONNECT_INIT_SCRIPT_WAIT_BODY = """
# Wait for Kafka Connect listener
echo "Waiting for Kafka Connect to start listening at ${KAFKA_CONNECT_CLUSTER_URI}... ⏳"
while : ; do
curl_status=$(curl -s -o /dev/null -w %{http_code} ${KAFKA_CONNECT_CLUSTER_URI}/connectors)
echo -e $(date) " Kafka Connect listener HTTP state: " $curl_status " (waiting for 200)"
if [ $curl_status -eq 200 ] ; then
    break
fi
sleep 5
done
echo -e Creating connectors...
"""

_KAFKA_CONNECT_INIT_SCRIPT_CONNECTOR_INSTANCE_TPL = """
curl -s -X POST -H "Content-Type:application/json" ${{KAFKA_CONNECT_CLUSTER_URI}}/connectors \\
    -d @/{config_dir}/{connector_json_file}.json
"""

def kafka_connect_init_script_file(name, connectors, connector_config_dir, out = None):
    """
    Creates a custom kafka connect initialization script file
    """
    if not out:
        out = "{name}.sh".format(name = name)

    write_file(
        name = name,
        out = out,
        content = [
            _KAFKA_CONNECT_INIT_SCRIPT_WAIT_BODY,
        ] + [
            _KAFKA_CONNECT_INIT_SCRIPT_CONNECTOR_INSTANCE_TPL.format(
                config_dir = connector_config_dir,
                connector_json_file = connector.split(":")[-1],
            )
            for connector in connectors
        ],
        is_executable = True,
    )

def kafka_connect_init_pkg(name, exec_dir, connectors, connector_config_dir, out = None):
    """Creates and packages an initiliazation shell script for Kafka Connect

    The initialization script is auto generated by 'kafka_connect_init_script_file',
    then it is pacakged as a tar archive under 'exec_dir'

    Args:
      name: the name of the tar archive target
      exec_dir: the directory under which the initialization script will be packaged
      connectors: a list of 'kafka_connect_connector_instance' JSON file targets
                  which will determine which connector instances get created
                  by the initialization script
      connector_config_dir: the directory under which the connector JSON files
                            are packaged in the final image
      out: [Optional] the name of the output script. Defaults to <name>.sh.
    """

    # 'name' is the name of the final tar archive, not the name of the script target
    # even though the actual script file name will be <name>.sh.
    # This is done to avoid conflict with target names between the script target
    # and the archive target
    kafka_connect_init_script_name = "{name}-script".format(name = name)
    if not out:
        out = "{name}.sh".format(name = name)

    script_name = out

    # generate the source code for the initialization script
    kafka_connect_init_script_file(
        name = kafka_connect_init_script_name,
        out = script_name,
        connectors = connectors,
        connector_config_dir = connector_config_dir,
    )

    srcs = [":{name}".format(name = kafka_connect_init_script_name)]

    # create a manifest file from the initilization script
    # this is done in order to allow manifest mutation,
    # which evetually move the script file inside the tar
    # to 'exec_dir' directory
    mtree_spec(
        name = "{name}.mtree".format(name = name),
        # use the script target name
        srcs = srcs,
        # prevent runfiles packing into the tar
        # as we run the script directly and do not
        # require bazel's assistant inside the container
        include_runfiles = False,
    )

    # move the script file under 'exec_dir' directory
    mtree_mutate(
        name = "{name}.mut".format(name = name),
        mtree = ":{name}.mtree".format(name = name),
        package_dir = exec_dir,
        # remove the package directory path from the final
        # path of the script
        strip_prefix = native.package_name(),
    )

    # package the initialization script as a tar archive
    tar(
        name = name,
        mtree = ":{name}.mut".format(name = name),
        srcs = srcs,
        args = [
            "--exclude=*.bazel",
            "--exclude=WORKSPACE",
        ],
    )

    # add a safety assertion for the archive structure to check we
    # didn't screw up
    assert_archive_contains(
        name = "{name}-archive-structure-test".format(name = name),
        archive = ":{name}".format(name = name),
        type = "tar",
        expected = [
            "{exec_dir}/{script_name}".format(
                exec_dir = exec_dir,
                script_name = script_name,
            ),
        ],
    )

def kafka_connect_init_image(name, base, exec_dir, connectors, connector_config_dir, **kwargs):
    """Creates a custom Kafka Connect initialization image

    Takes a list of connector instances, generates an initialization script
    that puts them to the Kafka Connect cluster via its REST API,
    packages both shell script and the connector JSON files into
    tar archives and uses them as OCI image layers

    Typically, since we communicate with Kafka Connect's REST API
    we utilize an image that has HTTP tools, mainly curl,
    hence most of the time 'base' will be @curl.

    Args:
      name: target name for the OCI image.
            Every other sub-target that get instantiated via this rule
            will get a derived named out this argument
      base: Base OCI image to add the initialization and connector packages to
            Since this image is supposed to communicate with a Kafka Connect
            cluster over HTTP, @curl is the easiest solution to go for
      exec_dir: the directory into the initilaization script will be packaged to
      connectors: a list of 'kafka_connect_connector_instance' JSON targets
                  that will be packaged alongside the initialization script
      connector_config_dir: the directory into which the connector instance
                            confiugration files will be packaged to
      **kwargs: misc key-value arguments for oci_image
    """

    # Create a tar archive out of the connector json files
    # Files will sit under /connector_config_dir
    connectors_pkg_name = "{name}-connectors".format(name = name)
    kafka_connect_connectors_pkg(
        name = connectors_pkg_name,
        connectors = connectors,
        package_dir = connector_config_dir,
    )

    # Create and package an initialization script for Kafka Connect cluster
    # Initialization script should sit under <exec_dir>/<{name}-script.sh>
    init_script_pkg_name = "{name}-script".format(name = name)
    init_script_name = "{name}-script.sh".format(name = name)
    kafka_connect_init_pkg(
        name = init_script_pkg_name,
        out = init_script_name,
        exec_dir = exec_dir,
        connectors = connectors,
        connector_config_dir = connector_config_dir,
    )

    # Create the OCI image for Kafka Connect from the base image
    oci_image(
        name = name,
        base = base,
        entrypoint = [
            "sh",
            "-c",
            "/{exec_dir}/{init_script_name}".format(
                exec_dir = exec_dir,
                init_script_name = init_script_name,
            ),
        ],
        tars = [
            ":" + connectors_pkg_name,
            ":" + init_script_pkg_name,
        ] + kwargs.pop("tars", []),
        **kwargs
    )

def kafka_connect_file_source_connector_instance(name, connector_name, topic, root_dir, namespace, accepted_filetypes):
    """Creates a Kafka Connect file source connector

    Args:
      name: target name for the connector.
      connector_name: name of the connector.
      topic: kafka topic name
      root_dir: root directory to watch for file changes.
      namespace: the name of the project/department etc. to which the files belong to.
      accepted_filetypes: comma-separated list of accepted file types.
    """

    # Create the Kafka Connect connector instance
    kafka_connect_connector_instance(
        name = name,
        connector_name = connector_name,
        connector_config = {
            "connector.class": "org.apache.camel.kafkaconnector.file.CamelFileSourceConnector",
            "tasks.max": "1",
            # topic for existing or created files; deleted or modified files are handled
            # by the file watcher connector
            "topics": topic,
            "value.converter": "org.apache.kafka.connect.storage.StringConverter",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "key.converter.schemas.enable": False,
            "value.converter.schemas.enable": False,

            # looks for matching files also in subdirectories
            "camel.source.endpoint.recursive": True,
            # we don't want to delete or move files once they are processed
            # since these belong to the filesystem
            "camel.source.endpoint.noop": True,
            "camel.source.path.directoryName": root_dir,
            "camel.source.endpoint.includeExt": accepted_filetypes,

            #
            "camel.source.camelMessageHeaderKey": "CamelFileNameConsumed",

            # Key transformations (key should be the namespace name)
            # 1. b"<path>" -> {"path": <path>}
            # 2. {"path": <path>} -> {"path": <path>, "namespace": namespace}
            # 3. {"path": <path>, "namespace": namespace} -> {"namespace": namespace}
            # 3.b. {"namespace": namespace} -> Header{"NTILHeader.namespace": namespace} [copy]
            # 4. {"namespace": namespace} -> bnamespace
            #
            # dkabely: I know these are stupid transformations, but this connector will not
            # Create any key if not explicitly told so (see camel.source.camelMessageHeaderKey above),
            # and since there's no conveinent SMT way to manipulate the key
            # value beside JSON operations, this is the way to go
            "transforms": ",".join([
                # 1. b"<path>" -> {"path": <path>}
                "HoistKeyField",
                # 2. {"path": <path>} -> {"path": <path>, "namespace": namespace}
                "InsertNamespaceKey",
                # 3. {"path": <path>, "namespace": namespace} -> {"namespace": namespace}
                "RemovePathKey",
                # 3.b. {"namespace": namespace} -> Header{"NTILHeader.namespace": namespace}
                "copyNamespaceToHeader",
                # 4. {"namespace": namespace} -> bnamespace
                "ExtractNamespaceKeyField",
                # 5. Drop redundant Camel.* headers (not all of them though)
                "DropCamelHeaders",
                # 6. Insert CamelHeader.CamelFileEventType
                "InsertCamelFileEventTypeCreate",
            ]),
            # 1. b"<path>" -> {"path": <path>}
            "transforms.HoistKeyField.type": "org.apache.kafka.connect.transforms.HoistField$Key",
            "transforms.HoistKeyField.field": "path",

            # 2. {"path": <path>} -> {"path": <path>, "namespace": namespace}
            "transforms.InsertNamespaceKey.type": "org.apache.kafka.connect.transforms.InsertField$Key",
            "transforms.InsertNamespaceKey.static.field": "namespace",
            "transforms.InsertNamespaceKey.static.value": namespace,

            # 3. {"path": <path>, "namespace": namespace} -> {"namespace": namespace}
            "transforms.RemovePathKey.type": "org.apache.kafka.connect.transforms.ReplaceField$Key",
            "transforms.RemovePathKey.exclude": "path",

            # 3.b. {"namespace": namespace} -> Header{"NTILHeader.namespace": namespace} [copy]
            "transforms.copyNamespaceToHeader.type": "org.apache.kafka.connect.transforms.HeaderFrom$Key",
            "transforms.copyNamespaceToHeader.fields": "namespace",
            "transforms.copyNamespaceToHeader.headers": "NTILHeader.namespace",
            "transforms.copyNamespaceToHeader.operation": "copy",

            # 4. {"namespace": namespace} -> bnamespace
            "transforms.ExtractNamespaceKeyField.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
            "transforms.ExtractNamespaceKeyField.field": "namespace",

            # 5. Drop redundant Camel.* headers
            "transforms.DropCamelHeaders.type": "org.apache.kafka.connect.transforms.DropHeaders",
            "transforms.DropCamelHeaders.headers": ",".join([
                "CamelHeader.CamelFileNameConsumed",
                "CamelHeader.CamelFileNameOnly",
                "CamelHeader.CamelFileRelativePath",
                "CamelHeader.CamelFileLength",
                "CamelHeader.CamelFileParent",
                "CamelHeader.CamelFileAbsolute",
            ]),

            # 6. Insert CamelHeader.CamelFileEventType
            "transforms.InsertCamelFileEventTypeCreate.type": "org.apache.kafka.connect.transforms.InsertHeader",
            "transforms.InsertCamelFileEventTypeCreate.header": "CamelHeader.CamelFileEventType",
            "transforms.InsertCamelFileEventTypeCreate.value.literal": "CREATE",
        },
    )

def kafka_connect_file_watch_connector_instance(name, connector_name, topic, root_dir, namespace, events = "MODIFY,DELETE"):
    """Creates a Kafka Connect file watcher connector

    Sends events for EXISTING files

    Args:
      name: target name for the connector.
      connector_name: name of the connector.
      topic: kafka topic name
      root_dir: root directory to watch for file changes.
      namespace: the name of the project/department etc. to which the files belong to.
      events: comma-separated list of events to watch for (default: "MODIFY,DELETE").
    """

    # Create the Kafka Connect connector instance
    kafka_connect_connector_instance(
        name = name,
        connector_name = connector_name,
        connector_config = {
            "connector.class": "org.apache.camel.kafkaconnector.filewatchsource.CamelFilewatchsourceSourceConnector",
            "topics": topic,
            # The value here is just the full path name.
            # However, we cannot hoist this to a JSON as the object that's
            # being manipulated the this connector is JavaFile (a bug of the connector)
            # therefore and value transformation will be performed on the consumer
            # connector side
            "value.converter": "org.apache.kafka.connect.storage.StringConverter",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": False,
            "value.converter.schemas.enable": False,
            "tasks.max": "1",
            # 'CREATE' event will be handled by a corresponding CamelFileSourceConnector
            # that cannot DELETE and MODIFY events, so it is a complement for this connector
            # Also, the source connector will send existing files in the directory which this
            # connector cannot do
            "camel.kamelet.file-watch-source.filePath": root_dir,
            "camel.kamelet.file-watch-source.events": events,
            # This will will copy the reason the event was triggered (CREATE|MODIFY|DELETE)
            # and place it as the message key
            # This is important because it will create a key for the message
            # on top which we can apply SMT's. Otherwise, if the key is empty,
            # no SMTs can be applied (they all be noop)
            "camel.source.camelMessageHeaderKey": "CamelFileEventType",

            # Key transformations (key should be the namespace name)
            # 1. b"DELETE" -> {"reason": "DELETE"}
            # 2. {"reason": "DELETE"} -> {"reason": "DELETE", "namespace": namespace}
            # 3. {"reason": "DELETE", "namespace": namespace} -> {"namespace": namespace}
            # 3.b. {"namespace": namespace} -> Header{"NTILHeader.namespace": namespace} [copy]
            # 4. {"namespace": namespace} -> bnamespace
            #
            # dkabely: I know these are stupid transformations, but this connector will not
            # Create any key if not explicitly told so, and since there's no conveinent SMT way to
            # manipulate the key value beside JSON operations, this is the way to go
            "transforms": ",".join([
                # 1. b"DELETE" -> {"reason": "DELETE"},
                "HoistKeyField",
                # 2. {"reason": "DELETE"} -> {"reason": "DELETE", "namespace": namespace}
                "InsertNamespaceKey",
                # 3. {"reason": "DELETE", "namespace": namespace} -> {"namespace": namespace}
                "RemoveReasonKey",
                # 3.b. {"namespace": namespace} -> Header{"NTILHeader.namespace": namespace}
                "copyNamespaceToHeader",
                # 4. {"namespace": namespace} -> bnamespace
                "ExtractNamespaceKeyField",
                # 5. Drop redundant Camel.* headers (not all of them though)
                "DropCamelHeaders",
            ]),
            # 1. b"DELETE" -> {"reason": "DELETE"}
            "transforms.HoistKeyField.type": "org.apache.kafka.connect.transforms.HoistField$Key",
            "transforms.HoistKeyField.field": "reason",

            # 2. {"reason": "DELETE"} -> {"reason": "DELETE", "namespace": namespace}
            "transforms.InsertNamespaceKey.type": "org.apache.kafka.connect.transforms.InsertField$Key",
            "transforms.InsertNamespaceKey.static.field": "namespace",
            "transforms.InsertNamespaceKey.static.value": namespace,

            # 3. {"reason": "DELETE", "namespace": namespace} -> {"namespace": namespace}
            "transforms.RemoveReasonKey.type": "org.apache.kafka.connect.transforms.ReplaceField$Key",
            "transforms.RemoveReasonKey.exclude": "reason",

            # 3.b. {"namespace": namespace} -> Header{"NTIL.namespace": namespace} [copy]
            "transforms.copyNamespaceToHeader.type": "org.apache.kafka.connect.transforms.HeaderFrom$Key",
            "transforms.copyNamespaceToHeader.fields": "namespace",
            "transforms.copyNamespaceToHeader.headers": "NTILHeader.namespace",
            "transforms.copyNamespaceToHeader.operation": "copy",
            # 4. {"namespace": namespace} -> bnamespace
            "transforms.ExtractNamespaceKeyField.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
            "transforms.ExtractNamespaceKeyField.field": "namespace",

            # 5. Drop redundant Camel.* headers
            "transforms.DropCamelHeaders.type": "org.apache.kafka.connect.transforms.DropHeaders",
            "transforms.DropCamelHeaders.headers": ",".join([
                "CamelHeader.CamelFileNameConsumed",
                "CamelHeader.CamelFileNameOnly",
                "CamelHeader.CamelFileRelativePath",
                "CamelHeader.CamelFileLength",
                "CamelHeader.CamelFileParent",
                "CamelHeader.CamelFileAbsolute",
            ]),
        },
    )
