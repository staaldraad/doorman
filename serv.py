from gevent.wsgi import WSGIServer
from manage import app

http_server = WSGIServer(('', 5000), app, keyfile='privkey.pem', certfile='cert.pem')
http_server.serve_forever()
