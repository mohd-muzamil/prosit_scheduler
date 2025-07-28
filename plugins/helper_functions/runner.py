from subprocess import check_output, CalledProcessError, STDOUT

def system_call(command):
    """
    https://stackoverflow.com/a/47144897/9681577
    """
    try:
        output = check_output(command, stderr=STDOUT).decode()
        success = True
    except CalledProcessError as e:
        output = e.output.decode()
        success = False
    return output, success
