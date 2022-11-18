import socketio
from tqdm.notebook import tqdm

def ts(ticks, codes):

    socketEndpoint = 'https://stream.coindcx.com'

    true = True

    sios = [socketio.Client() for _ in range(len(codes))]

    for i in tqdm(range(len(codes))):
        sio = sios[i]
        channelName = codes[i]
        sio.connect(socketEndpoint, transports = 'websocket')
        sio.emit('join', { 'channelName': channelName })

        @sio.event
        def connect():
            pass

        @sio.on('depth-update')
        def on_message(response):
            val = eval(response['data'])
            ticks[val['channel']] = val