import logging
import subprocess
import time
import shlex

from config import (
    AZURE_DATABASE_LOCATION,
    AZURE_DATABASE_NAME,
    SQL_PASSWORD,
    SQL_USERNAME
)


def send_sp(cmd):
    pswd = str.encode('{}\n'.format(SQL_PASSWORD))
    proc = subprocess.Popen(shlex.split(cmd), stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    proc.stdin.write(pswd)
    out, err = proc.communicate()
    return out


def is_process_allowed(block_cmd):
    while 1:
        output = send_sp(block_cmd)
        if filter(type(output).isdigit, output) == '1':
            print("\t _sp_IsProcessAllowed == TRUE")
            break
        else:
            print("\t\t _sp_IsProcessAllowed == FALSE: {}".format(output.split()))
            time.sleep(5)
    return


def is_process_finished(block_cmd):
    while 1:
        o = send_sp(block_cmd)

        if filter(type(o).isdigit, o) == '1':
            print("\t _sp_IsProcessFinished == TRUE")
            break
        else:
            print("\t\t _sp_IsProcessFinished == FALSE: {}".format(o.split()))
            time.sleep(5)
    return


def main():
    try:
        while True:
            print("NORM DATA START")

            check_allowed_cmd = 'sqlcmd -Stcp:{0} -d "{1}" -U{2} -Q"exec ProcessControl._sp_IsProcessAllowed"'.format(
                AZURE_DATABASE_LOCATION,
                AZURE_DATABASE_NAME,
                SQL_USERNAME
            )
            is_process_allowed(check_allowed_cmd)

            check_completion_cmd = 'sqlcmd -Stcp:{0} -d "{1}" -U{2} -Q"exec [Logging.Select]._sp_IsProcessFinished"'.format(
                AZURE_DATABASE_LOCATION,
                AZURE_DATABASE_NAME,
                SQL_USERNAME
            )
            is_process_finished(check_completion_cmd)

            print("\t START NORMALIZE DATA")
            sp_load_data_cmd = 'sqlcmd -Stcp:{0}-d "{1}" -U{2} -Q"exec Import._sp_NormalizeData"'.format(
                AZURE_DATABASE_LOCATION,
                AZURE_DATABASE_NAME,
                SQL_USERNAME
            )
            send_sp(sp_load_data_cmd)
            print("FINISHED CALL")

    except Exception as e:
        logging.warning("LOG ERROR - {0}".format(e))


if __name__ == '__main__':
    main()
