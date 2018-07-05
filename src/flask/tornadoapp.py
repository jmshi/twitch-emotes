#! /usr/bin/env python

from tornado.wsgi import WSGIContainer
from tornado.ioloop import IOLoop
from tornado.web import FallbackHandler, RequestHandler, Application
from app import app
import logging

class MainHandler(RequestHandler):
 def get(self):
   self.write("This message comes from Tornado ^_^")

tr = WSGIContainer(app)
logging.getLogger('tornado.access').disabled = False #True
application = Application([
(r"/tornado", MainHandler),
(r".*", FallbackHandler, dict(fallback=tr)),
])

if __name__ == "__main__":
 application.listen(80)
 IOLoop.instance().start()
