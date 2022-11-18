import socketio

def ts(ticks, channelName):

    socketEndpoint = 'https://stream.coindcx.com'
    true = True
    sio = socketio.Client()
    sio.connect(socketEndpoint, transports = 'websocket')
    sio.emit('join', { 'channelName': channelName })

    @sio.event
    def connect():
        pass

    @sio.on('depth-update')
    def on_message(response):
        val = eval(response['data'])
        ticks[val['channel']] = val