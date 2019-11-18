import requests
from requests.auth import HTTPBasicAuth
import argparse

USAGE = """
cleanup_betas.py [<command_args>]
"""

def _arguments(parser):
    """Arguments relevant for every CLI command"""

    parser.add_argument("--latest-release", default=None,
                       help="Release version for which betas should be cleaned up (mandatory)")
    parser.add_argument("--bintray-user", default=None,
                       help="Bintray username (mandatory)")
    parser.add_argument("--bintray-api-key", default=None, help="Bintray API key (mandatory)")
    parser.add_argument("--noop", help="Performs a blank run that will only list"
                                        " the versions that would be deleted (optional)", action="store_true")
    parser.add_argument("--current-beta-cleanup", help="Keep only the latest beta", action="store_true")

def _parse_arguments():
    parser = argparse.ArgumentParser(description="", usage=USAGE)
    _arguments(parser)
    args = parser.parse_args()
    if args.latest_release == None or args.bintray_user == None or args.bintray_api_key == None:
        print("Missing parameters!")
        print(parser.format_help())
        exit()
    return args

def _get_candidates(args, bintray_response):
    candidates = []
    i = 0
    for version in res.json()['versions']:
            if version.startswith(args.latest_release) and ("BETA" in version or "SNAPSHOT" in version) :
                i += 1
                if args.current_beta_cleanup and i == 1:
                    continue
                else:
                    candidates.append(version)

    return candidates

if __name__ == '__main__':
    args = _parse_arguments()
    for repository in ['reaper-deb-beta', 'reaper-rpm-beta', 'reaper-maven-beta', 'reaper-tarball-beta']:
        if "maven" in repository:
            print("calling " + 'https://api.bintray.com/packages/thelastpickle/{0}/io.cassandrareaper%3Acassandra-reaper'.format(repository))
            res = requests.get('https://api.bintray.com/packages/thelastpickle/{0}/io.cassandrareaper%3Acassandra-reaper'.format(repository), auth=HTTPBasicAuth(args.bintray_user, args.bintray_api_key))
        else:
            print("calling " + 'https://api.bintray.com/packages/thelastpickle/{0}/cassandra-reaper-beta'.format(repository))
            res = requests.get('https://api.bintray.com/packages/thelastpickle/{0}/cassandra-reaper-beta'.format(repository), auth=HTTPBasicAuth(args.bintray_user, args.bintray_api_key))
        #print res.text
        print("Candidates for deletion in {0}:".format(repository))
        candidates = _get_candidates(args, res)
        for candidate in candidates:
            print(candidate)
        
        if not args.noop:
            for version in candidates:
                print('Deleting {0} {1}...'.format(repository, version))
                if "maven" in repository:
                    res = requests.delete('https://api.bintray.com/packages/thelastpickle/{0}/io.cassandrareaper%3Acassandra-reaper/versions/{1}'.format(repository, version), auth=HTTPBasicAuth(args.bintray_user, args.bintray_api_key))
                else:
                    res = requests.delete('https://api.bintray.com/packages/thelastpickle/{0}/cassandra-reaper-beta/versions/{1}'.format(repository, version), auth=HTTPBasicAuth(args.bintray_user, args.bintray_api_key))
    
    print("All done")


