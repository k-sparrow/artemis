load("@aspect_bazel_lib//lib:tar.bzl", "mtree_mutate", "mtree_spec", "tar")
load("@aspect_bazel_lib//lib:testing.bzl", "assert_archive_contains")
load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_push", oci_load = "oci_tarball")

def kafka_connect_image(*, name, base, connectors_srcs, cp_components_dir, **kwargs):
    exposed_ports = kwargs.pop("exposed_ports", ["8083"])

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

    json
    {
        "name": <connector_name>,
        "config": <config>
    }
    

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