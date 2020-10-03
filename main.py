import multiprocessing
import fileinput
import socket

socket.setdefaulttimeout(2)

def worker(i):
    i+= 1
    with open(f"./parsed_data/{i}", "a") as save_file:
        for line in fileinput.input(f"./data/{i}"):
            domain = '.'.join( list( reversed( line.split('\t')[1].split('.') ) ) )

            try:
                ip_list = socket.gethostbyname(domain)
                save_file.write(f"{domain},{ip_list}\n")
                print(ip_list)
            except Exception as e:
                pass

        save_file.close()


if __name__ == '__main__':
    jobs = []
    for i in range(20):
        p = multiprocessing.Process(target=worker, args=(i,))
        jobs.append(p)
        p.start()