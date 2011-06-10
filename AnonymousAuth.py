from boto.auth_handler import AuthHandler

class AnonymousAuthHandler(AuthHandler):
	def __init__(self, host, config, provider):
		AuthHandler.__init__(self, host, config, provider)

	def add_auth(self, http_request, **kwargs):
		return # Nothing to do for anonymous access!
