from collections import namedtuple

from dtest import debug

# UpgradePath's contain data about upgrade paths we wish to test
# They also contain VersionMeta's for each version the path is testing
UpgradePath = namedtuple('UpgradePath', ('name', 'starting_version', 'upgrade_version', 'starting_meta', 'upgrade_meta'))

# VersionMeta's capture data about version lines, protocols supported, and current version identifiers
# they should have a 'variant' value of 'current', 'indev', or 'next':
#    'current' means most recent released version, 'indev' means where changing code is found, 'next' means a tentative tag
VersionMeta = namedtuple('VersionMeta', ('name', 'variant', 'version', 'min_proto_v', 'max_proto_v'))

indev_2_0_x = None  # None if release not likely
current_2_0_x = VersionMeta(name='current_2_0_x', variant='current', version='2.0.17', min_proto_v=1, max_proto_v=2)
next_2_0_x = None  # None if not yet tagged

indev_2_1_x = VersionMeta(name='indev_2_1_x', variant='indev', version='git:cassandra-2.1', min_proto_v=1, max_proto_v=3)
current_2_1_x = VersionMeta(name='current_2_1_x', variant='current', version='2.1.14', min_proto_v=1, max_proto_v=3)
next_2_1_x = None  # None if not yet tagged

indev_2_2_x = VersionMeta(name='indev_2_2_x', variant='indev', version='git:cassandra-2.2', min_proto_v=1, max_proto_v=4)
current_2_2_x = VersionMeta(name='current_2_2_x', variant='current', version='2.2.6', min_proto_v=1, max_proto_v=4)
next_2_2_x = None  # None if not yet tagged

indev_3_0_x = VersionMeta(name='indev_3_0_x', variant='indev', version='git:cassandra-3.0', min_proto_v=3, max_proto_v=4)
current_3_0_x = VersionMeta(name='current_3_0_x', variant='current', version='3.0.5', min_proto_v=3, max_proto_v=4)
next_3_0_x = None  # None if not yet tagged

indev_3_x = VersionMeta(name='indev_3_x', variant='indev', version='git:cassandra-3.7', min_proto_v=3, max_proto_v=4)
current_3_x = VersionMeta(name='current_3_x', variant='current', version='3.5', min_proto_v=3, max_proto_v=4)
next_3_x = None  # None if not yet tagged

head_trunk = VersionMeta(name='head_trunk', variant='indev', version='git:trunk', min_proto_v=3, max_proto_v=4)


# maps an VersionMeta representing a line/variant to a list of other UpgradeMeta's representing supported upgrades
MANIFEST = {
    # commented out until we have a solution for specifying java versions in upgrade tests
    # indev_2_0_x:                [indev_2_1_x, current_2_1_x, next_2_1_x],
    # current_2_0_x: [indev_2_0_x, indev_2_1_x, current_2_1_x, next_2_1_x],
    # next_2_0_x:                 [indev_2_1_x, current_2_1_x, next_2_1_x],

    indev_2_1_x:                [indev_2_2_x, current_2_2_x, next_2_2_x, indev_3_0_x, current_3_0_x, next_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],
    current_2_1_x: [indev_2_1_x, indev_2_2_x, current_2_2_x, next_2_2_x, indev_3_0_x, current_3_0_x, next_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],
    next_2_1_x:                 [indev_2_2_x, current_2_2_x, next_2_2_x, indev_3_0_x, current_3_0_x, next_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],

    indev_2_2_x:                [indev_3_0_x, current_3_0_x, next_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],
    current_2_2_x: [indev_2_2_x, indev_3_0_x, current_3_0_x, next_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],
    next_2_2_x:                 [indev_3_0_x, current_3_0_x, next_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],

    indev_3_0_x:                [indev_3_x, current_3_x, next_3_x, head_trunk],
    current_3_0_x: [indev_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],
    next_3_0_x:                 [indev_3_x, current_3_x, next_3_x, head_trunk],

    indev_3_x:              [head_trunk],
    current_3_x: [indev_3_x, head_trunk],
    next_3_x:               [head_trunk],
}


def build_upgrade_pairs():
    """
    Using the manifest (above), builds a set of valid upgrades, according to current testing practices.

    Returns a list of UpgradePath's.
    """
    valid_upgrade_pairs = []
    # single upgrades from one version to another
    for origin_ver, destination_vers in MANIFEST.items():
        # for now we only test upgrades of these types:
        #    current -> in-dev (aka: released -> branch)
        #    current -> next (aka: released -> proposed release point)
        #    next -> in-dev (aka: proposed release point -> branch)
        for destination_ver in destination_vers:
            if (origin_ver is None) or (destination_ver is None):
                debug("skipping class creation as a version is undefined (this is normal), versions: {} and {}".format(origin_ver, destination_ver))
                continue

            # first check if the upgrade pair is one we want to test
            if (origin_ver.variant == 'current' and destination_ver.variant == 'indev') or (origin_ver.variant == 'current' and destination_ver.variant == 'next') or (origin_ver.variant == 'next' and destination_ver.variant == 'indev'):
                if origin_ver.max_proto_v >= destination_ver.min_proto_v:
                    valid_upgrade_pairs.append(
                        UpgradePath(
                            name='Upgrade_' + origin_ver.name + '_To_' + destination_ver.name,
                            starting_version=origin_ver.version,
                            upgrade_version=destination_ver.version,
                            starting_meta=origin_ver,
                            upgrade_meta=destination_ver
                        )
                    )
                else:
                    debug("skipping class creation, no compatible protocol version between {} and {}".format(origin_ver.name, destination_ver.name))
            else:
                debug("skipping class creation, no testing of '{}' to '{}' (for {} upgrade to {})".format(origin_ver.variant, destination_ver.variant, origin_ver.name, destination_ver.name))

    return valid_upgrade_pairs
