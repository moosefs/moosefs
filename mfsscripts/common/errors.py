class MFSCommunicationError(RuntimeError):
	def __init__(self, message=""):
		super().__init__("MooseFS communication error" + (": " + message if message else ""))
