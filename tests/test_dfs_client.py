from yadfs.client.client import Client
import sys
import cfg

if __name__ == '__main__':
    cfg_path = sys.argv[1]
    opts = cfg.load(cfg_path)
    cl = Client(opts['ns_addr'])
    cl.create_file("/home/osboxes/yamr/cfg.py", "/test")
    data = cl.get_chunk("/test/cfg.py_0")

    cl.download_to("/test/cfg.py", "/home/osboxes/yamr/cfg_text.py")
    print(data)

    cl.save("hohohoho", "/test/my_file")
    cl.download_to("/test/my_file", "/home/osboxes/yamr/hoho.txt")
