import multiprocessing
import fileinput
import socket

socket.setdefaulttimeout(2)

def worker(i):
    with open(f"./parsed_data/{i}", "w+") as save_file:
        filename = (str(i).zfill(len(str(111))))
        for line in fileinput.input(f"./data/{filename}"):
            domain = '.'.join( list( reversed( line.split('\t')[1].split('.') ) ) )

            try:
                ip_list = socket.gethostbyname(domain)
                save_file.write(f"{domain},{ip_list}\n")
                print(ip_list)
            except Exception as e:
                pass


if __name__ == '__main__':
    jobs = []
    for i in range(40):
        p = multiprocessing.Process(target=worker, args=(i,))
        jobs.append(p)
        p.start()