"""
Apache Kafka KSQL-DB macro utilities for building a KSQL-DB image with
an initialization script
"""

load("@aspect_bazel_lib//lib:tar.bzl", "mtree_mutate", "mtree_spec", "tar")
load("@aspect_bazel_lib//lib:testing.bzl", "assert_archive_contains")
load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@rules_oci//oci:defs.bzl", "oci_image")

# This is the KSQL-DB init script that will be run when the container starts.
# Taken and modified from Robin Moffatt's ksql-docker project:
# https://rmoff.net/2018/12/15/docker-tips-and-tricks-with-kafka-connect-ksqldb-and-kafka/#execute-a-ksql-script-through-ksql-cli
_KAFKA_CONNECT_INIT_SCRIPT_WAIT_BODY = """
echo -e "\n\n⏳ Waiting for KSQL to be available before launching CLI\n"
while [ $$(curl -s -o /dev/null -w %{http_code} ${KSQLDB_SERVER_URL}) -eq 000 ]
do 
  echo -e $$(date) "KSQL Server HTTP state: " $$(curl -s -o /dev/null -w %{http_code} ${KSQLDB_SERVER_URL}) " (waiting for 200)"
  sleep 5
done
echo -e "\n\n-> Running KSQL commands\n"
cat {script_dir}/{init_script} <(echo 'EXIT')| ksql ${KSQLDB_SERVER_URL}
echo -e "\n\n-> Sleeping…\n"
sleep infinity
"""

def ksqldb_init_script(name, script_dir, out = None):
    """
    Generates a KSQL-DB init script.

    Args:
        name: The name of the target.
        script_dir: The directory where the KSQL scripts will be placed.
        out: The output file path for the generated script.
    """
    if not out:
        out = "{name}.sh".format(name = name)

    init_script_name = out
    write_file(
        name = name,
        out = out,
        content = [
            _KAFKA_CONNECT_INIT_SCRIPT_WAIT_BODY.format(
                script_dir = script_dir,
                init_script = init_script_name,
            ),
        ],
        is_executable = True,
    )

def ksqldb_init_image_pkg(name, script_name, script_dir):
    """
    Generates a KSQL-DB init image package that includes the KSQL init script.

    Args:
        name: The name of the target.
        script_name: The name of the KSQL init script.
        script_dir: The directory where the KSQL scripts will be placed.
    """
    ksqldb_init_script_name = "{name}-init-script".format(name = name)

    ksqldb_init_script(
        name = ksqldb_init_script_name,
        script_dir = script_dir,
        out = script_name,
    )

    srcs = [":{name}".format(name = ksqldb_init_script_name)]

    # create a manifest file from the initilization script
    # this is done in order to allow manifest mutation,
    # which evetually move the script file inside the tar
    # to 'script_dir' directory
    mtree_spec(
        name = "{name}.mtree".format(name = name),
        # use the script target name
        srcs = srcs,
        # prevent runfiles packing into the tar
        # as we run the script directly and do not
        # require bazel's assistance inside the container
        include_runfiles = False,
    )

    # move the script file under 'script_dir' directory
    mtree_mutate(
        name = "{name}.mut".format(name = name),
        mtree = ":{name}.mtree".format(name = name),
        package_dir = script_dir,
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
            "{script_dir}/{script_name}".format(
                script_dir = script_dir,
                script_name = script_name,
            ),
        ],
    )

def ksqldb_cli_init_image(
        name,
        base,
        script_dir,
        **kwargs):
    """
    Creates a KSQL-DB CLI initialization image that includes the KSQL init script.

    Args:
        name: The name of the target.
        base: The base image to use for KSQL-DB CLI.
        script_dir: The directory where the KSQL scripts will be placed.
        **kwargs: Additional keyword arguments for the oci_image rule.
    """
    init_script_pkg_name = "{name}-script".format(name = name)
    init_script_name = "{name}-script.sh".format(name = name)

    ksqldb_init_image_pkg(
        name = name,
        script_dir = script_dir,
        script_name = init_script_name,
    )

    oci_image(
        name = name,
        base = base,
        entrypoint = [
            "sh",
            "-c",
            "/{script_dir}/{script_name}".format(
                script_dir = script_dir,
                script_name = init_script_name,
            ),
        ],
        tars = [
            ":{name}".format(name = init_script_pkg_name),
        ] + kwargs.pop("tars", []),
        **kwargs
    )
