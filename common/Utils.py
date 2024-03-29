from datetime import datetime
import socket, os


class Utils():
    
    # def __init__(self) -> None:
    #     pass

    @staticmethod
    def sizeof_fmt(num:int) -> str:
        for unit in ['B','KB','MB','GB','TB','PB','EB','ZB'] :
            if abs(num) < 1024.0:                
                return format(num, '3.1f')+unit
            num /= 1024.0
        return format(num, '0.1f')+unit

    @staticmethod
    def nowtime() -> str:
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def get_ip() -> str:
        # win / centos 공통사용가능
        # UDP 커넥을 이용하는 방법으로 하면 바로 찾을수있다. (Private IP 인 경우에만 이며, Public은 외부 응답으로 가져와야한다)
        # 출처: https://see-ro-e.tistory.com/173
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 0)) 
        ip = s.getsockname()[0] 
        return ip

    @staticmethod
    def make_dir(path:str, mode:str) -> None:
        if not os.path.isdir(path): 
            oldmask = os.umask(0)
            os.makedirs(path, 0o775)
            os.umask(oldmask)