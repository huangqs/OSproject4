import json
conf = json.load(open('conf/settings.conf'))
ports = conf['ports']
nips = [conf['n01'], conf['n02'], conf['n03'], conf['n04'],conf['n05']]
loch = '127.0.0.1'

import urllib.parse
import urllib.request
import time
import _thread

class Test():
    
    
    
    def GetTest(self, key, ip, port):
        get = urllib.request.Request('http://'+ str(ip) + ':' + str(port)
                                     +'/kv/get?key=' + key)
        res = urllib.request.urlopen(get).read()
        return res
        
    def PostTest(self, method, ip, port, key, value = 0):
        url = 'http://'+ str(ip) + ':' + str(port) + '/kv/' + method
        if method == 'delete':
            postdata = urllib.parse.urlencode({'key': key})
        else:
            postdata = urllib.parse.urlencode({'key': key, 'value': value})
        postdata = postdata.encode('utf-8')
        res = urllib.request.urlopen(url,postdata)
        temp = res.read()
        return temp
    def CountkeyTest(self, ip, port):
        get = urllib.request.Request('http://'+ str(ip) + ':' + str(port)+'/kvman/countkey')
        res = urllib.request.urlopen(get).read()
        return res
    
    def run(self):
        for i in range(20):
            for j in range(5):
                _thread.start_new_thread( self.PostTest, ('insert',loch, ports[j], str(i+20), j ) )
        time.sleep(10)
        print(0)
    
    print(1)
if __name__ == '__main__':
    Test().run()
