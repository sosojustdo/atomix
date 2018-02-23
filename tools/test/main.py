from cluster import get_cluster, set_cluster, Cluster
from test import Test
from errors import TestError
import sys

def run():
    """Runs the test framework."""
    import argparse

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='action', help="The action to execute")

    setup_parser = subparsers.add_parser('setup', help="Setup a test cluster")
    setup_parser.add_argument('name', help="The cluster name")
    setup_parser.add_argument('--nodes', '-n', type=int, default=3, help="The number of nodes in the cluster")
    setup_parser.add_argument('--subnet', '-s', default='172.18.0.0/16', help="The subnet in which to create the cluster")
    setup_parser.add_argument('--gateway', '-g', default=None, help="The IPv4 gateway for the master subnet")

    teardown_parser = subparsers.add_parser('teardown', help="Tear down a test cluster")
    teardown_parser.add_argument('name', nargs='?', help="The cluster name")

    add_node_parser = subparsers.add_parser('add-node', help="Add a node to a test cluster")
    add_node_parser.add_argument('cluster', nargs='?', help="The cluster to which to add the node")
    add_node_parser.add_argument('--type', '-t', choices=['server', 'client'], default='server', help="The type of node to add")

    remove_node_parser = subparsers.add_parser('remove-node', help="Remove a node from a test cluster")
    remove_node_parser.add_argument('node', help="The node to remove from the cluster")
    remove_node_parser.add_argument('--cluster', '-c', help="The cluster from which to remove the node")

    cluster_parser = subparsers.add_parser('cluster-info', help="Get information about a test cluster")
    cluster_parser.add_argument('cluster', nargs='?', help="The cluster for which to get information")

    run_parser = subparsers.add_parser('run', help="Run a test")
    run_parser.add_argument('test', help="The test to run")
    run_parser.add_argument('--cluster', '-c', help="The cluster on which to run the test")

    args = parser.parse_args()

    try:
        if args.action == 'setup':
            Cluster(args.name).setup(args.nodes, args.subnet)
        elif args.action == 'teardown':
            cluster = get_cluster(args.name)
            cluster.teardown()
            cluster.cleanup()
        elif args.action == 'add-node':
            get_cluster(args.cluster).add_node(args.type)
        elif args.action == 'remove-node':
            get_cluster(args.cluster).remove_node(args.node)
        elif args.action == 'cluster-info':
            print get_cluster(args.cluster)
        elif args.action == 'run':
            set_cluster(args.cluster)
            try:
                Test(args.test).run()
            except AssertionError, e:
                sys.exit(1)
            else:
                sys.exit(0)
    except TestError, e:
        print str(e)
        sys.exit(1)
    else:
        sys.exit(0)

def _import_colorizor():
    from colorama import init
    init()

def _import_tests():
    import testtest

if __name__ == '__main__':
    _import_colorizor()
    _import_tests()
    run()