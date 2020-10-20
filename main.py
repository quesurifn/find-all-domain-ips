import multiprocessing
import fileinput
import socket
from datetime import datetime


socket.setdefaulttimeout(2)

def worker(i):
    filename = str(i).zfill(len(str(111)))
    buffer_array = []
    for line in fileinput.input(f"./data/{filename}"):
        domain = '.'.join( reversed( line.split('\t')[1].split('.') ) )
        try:
            ip_list = socket.gethostbyname(domain)
            buffer_array.append(f"{domain},{ip_list}")
            if len(buffer_array) == 1000:
                with open(f"./parsed_data/{i}", "a+") as save_file:
                    save_file.write("\n".join(buffer_array))
                    buffer_array = []
                    print(f"{datetime.now()} -- WRITING: worker {i}")

        except Exception as e:
            pass

    with open(f"./parsed_data/{i}", "a+") as save_file:
        save_file.write("\n".join(buffer_array))
        print(f"{datetime.now()} -- WRITING ***FINAL***: worker {i}")

if __name__ == '__main__':
    jobs = []
    for i in range(87):
        p = multiprocessing.Process(target=worker, args=(i,))
        jobs.append(p)
        p.start()