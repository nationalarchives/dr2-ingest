import sys

progress_message_active = False

def progress(text):
    global progress_message_active
    if progress_message_active:
        sys.stdout.write("\r" + " " * 200 + "\r")
    sys.stdout.write(text)
    sys.stdout.flush()
    progress_message_active = True


def message(text):
    global progress_message_active
    # If a progress line is active, remove it before printing message
    if progress_message_active:
        sys.stdout.write("\r" + " " * 200 + "\r")
        progress_message_active = False
    print(text)
