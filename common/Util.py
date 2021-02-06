
class Util():
    def __init__(self):
        pass

    # @staticmethod
    def sizeof_fmt(num:int):        
        for unit in ['B','KB','MB','GB','TB','PB','EB','ZB'] :
            if abs(num) < 1024.0:                
                return format(num, '3.1f')+unit
            num /= 1024.0
        return format(num, '0.1f')+unit