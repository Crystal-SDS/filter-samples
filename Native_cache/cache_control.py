SSD_PATH = "/mnt/ssd/"


GET
resp_headers = {}
## CHECK IF FILE IS IN CAHCE ##

if os.path.exists(SSD_PATH) and self.storlet_list and 'cache-1.0.jar' in self.storlet_list:
    
    self.storlet_list.remove('cache-1.0.jar')
    
    object_path = self.request.environ['PATH_INFO']
    object_id = (hashlib.md5(object_path).hexdigest())

    object_id, object_size, object_etag = self.cache.access_cache("GET", object_id)

    if object_id:
        self.logger.info('SDS Storlets - Object '+object_path+' in cache')
        resp_headers = {}
        resp_headers['content-length'] = str(object_size)                
        resp_headers['etag'] = object_etag
        
        cached_object = open(SSD_PATH+object_id,'r')
        
        return Response(app_iter=cached_object,
                        headers=resp_headers,
                        request=self.request) 
        
###############################


def _copy_on_cache(self, object_id,reader, writer):
    cahed_object = open(SSD_PATH+object_id, 'w');
    for chunk in iter(lambda: reader(65536), ''):
        writer.write(chunk)
        cahed_object.write(chunk)
    writer.close()
    cahed_object.close()


    ## PUT OBJECT IN CAHCE ##
    if os.path.exists(SSD_PATH) and  self.storlet_list and 'cache-1.0.jar' in self.storlet_list:
        
        self.storlet_list.remove('cache-1.0.jar')
        
        object_path = self.request.environ['PATH_INFO']
        object_size = int(self.request.headers.get('Content-Length',''))
        object_etag = self.request.headers.get('ETag','')
        object_id = (hashlib.md5(object_path).hexdigest())
        
        to_evict = self.cache.access_cache("PUT", object_id, object_size, object_etag)

        for object_id in to_evict:
            os.remove(SSD_PATH+object_id)
        
        r, w = os.pipe()
        write_pipe = os.fdopen(w,'w') 
        read_pipe = self.request.environ['wsgi.input'].read
        self.request.environ['wsgi.input'] = sc.IterLike(r, 10)
        
        threading.Thread(target=self._copy_on_cache,args=(object_id,read_pipe,write_pipe)).start()

        self.logger.info('SDS Storlets - Object '+object_path+' stored in cache with ID: '+object_id)
    ###############################