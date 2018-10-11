import subprocess
import shlex

from config import (
    AZURE_DATABASE_LOCATION,
    AZURE_DATABASE_NAME,
    SQL_PASSWORD
)


cmd1 = 'sqlcmd -Stcp:{0} -d "{1}" -U{2} -Q"truncate table Staging.NormalizedStationData"'.format(
    AZURE_DATABASE_LOCATION,
    AZURE_DATABASE_NAME,
    SQL_PASSWORD
)
cmd2 = 'sqlcmd -Stcp:{0} -d "{1}" -U{2} -Q"truncate table [Staging.Exceptions].DuplicateStationData"'.format(
    AZURE_DATABASE_LOCATION,
    AZURE_DATABASE_NAME,
    SQL_PASSWORD
)
cmd3 = 'sqlcmd -Stcp:{0} -d "{1}" -U{2} -Q"truncate table Staging.RawStationData"'.format(
    AZURE_DATABASE_LOCATION,
    AZURE_DATABASE_NAME,
    SQL_PASSWORD
)


def command(cmd):
    pswd = str.encode('{}\n'.format(SQL_PASSWORD))
    p = subprocess.Popen(shlex.split(cmd), stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    p.stdin.write(pswd)
    p.communicate()


command(cmd1)
command(cmd2)
command(cmd3)
print("finished")
