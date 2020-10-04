import threading
import fileinput
import socket
from datetime import datetime

socket.setdefaulttimeout(2)

def worker(i):
    with open(f"./parsed_data/{i}", "a+") as save_file:
        filename = (str(i).zfill(len(str(111))))
        buffer_array = []
        for line in fileinput.input(f"./data/{filename}"):
            domain = '.'.join( list( reversed( line.split('\t')[1].split('.') ) ) )

            try:
                ip_list = socket.gethostbyname(domain)
                buffer_array.append(ip_list)
                if(len(buffer_array) == 1000):
                    save_file.write("\n".join(buffer_array))
                    buffer_array = []
                    print (f"{datetime.now()} -- WRITING: worker {i}")
            except Exception as e:
                pass

        save_file.write("\n".join(buffer_array))
        save_file.close()


if __name__ == '__main__':
    for i in range(87):
        t = threading.Thread(target=worker, args=(i,))
        t.start()