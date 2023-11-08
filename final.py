import paho.mqtt.client as mqtt
import time
import ssl
import json
import minimalmodbus

MODBUS_ADDRESS = 1
SERIAL_PORT = '/dev/ttyUSB0'

meter = minimalmodbus.Instrument(SERIAL_PORT, MODBUS_ADDRESS)
meter.serial.baudrate = 9600
meter.serial.bytesize = 8
meter.serial.parity = minimalmodbus.serial.PARITY_NONE
meter.serial.stopbits = 1
meter.serial.timeout = 0.1

def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to broker")
            global Connected #Use global variable
            Connected = True #Signal connection
        else:
            print("Connection failed")

def on_publish(client, userdata, result):
    print(f'Data published. Data: {attribute_value}')
pass

def on_message(client, userdata, message):
    msg = json.loads(message.payload.decode('utf-8'))
    id = msg['attributeState']['ref']['id']
    att_name = msg["attributeState"]['ref']['name']
    att_value = msg["attributeState"]["value"]
    print(f'Message received. Asset ID: {id}. Attribute name: {att_name}. Attribute value: {att_value}')



def read_2registers(starting_register_address):
    try:
        value_high= meter.read_register(starting_register_address, functioncode=3)
        value_low = meter.read_register(starting_register_address+1,functioncode=3)
        value_32=(value_high<<16)+ value_low
        return value_32
    except minimalmodbus.ModbusException as e:
        print("Modbus communication error:", e)
        return None

def read_1register(register_address):
    try:
        value = meter.read_register(register_address, functioncode=3)
        return value
    except minimalmodbus.ModbusException as e:
        print("Modbus communication error:", e)
        return None

Connected = False
username = 'master:mqttuser'
secret = 'YMOMT6KEQcDLKAFHIVYHpj0lNPrPqEqb'
host = '192.168.1.8'
port = 8883
clientID = 'mqtt'


assetIDwr = '6uVLJB0ZaHQb24Tgy7G6UF'

attributeWr = 'data'

clientMQTT = mqtt.Client(clientID)
clientMQTT.username_pw_set(username, password = secret)

context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
context.check_hostname = False
clientMQTT.tls_set_context(context)

clientMQTT.on_connect = on_connect
clientMQTT.on_publish = on_publish
clientMQTT.on_message = on_message
clientMQTT.connect(host, port=port)
clientMQTT.loop_start()

while Connected != True:
    time.sleep(0.1)
    
try:
    while True:
        data = {"data":[{
            "U1":"".join(format(read_2registers(0x016E)*0.0001, ".2f")),
            "U2":"".join(format(read_2registers(0x0170)*0.0001, ".2f")),
            "U3":"".join(format(read_2registers(0x0172)*0.0001, ".2f")),
            "U12":"".join(format(read_2registers(0x016E)*0.0001*1.732, ".2f")),
            "U23":"".join(format(read_2registers(0x0170)*0.0001*1.732, ".2f")),
            "U31":"".join(format(read_2registers(0x0172)*0.0001*1.732, ".2f")),
            "I1":"".join(format(read_2registers(0x0174)*0.0001, ".2f")),
            "I2":"".join(format(read_2registers(0x0176)*0.0001, ".2f")),
            "I3":"".join(format(read_2registers(0x0178)*0.0001, ".2f")),
            "P1":"".join(format(read_2registers(0x017C)*0.0001, ".2f")),
            "P2":"".join(format(read_2registers(0x017E)*0.0001, ".2f")),
            "P3":"".join(format(read_2registers(0x0180)*0.0001, ".2f")),
            "P":"".join(format(read_2registers(0x017A)*0.0001, ".2f")),
            "Q1":"".join(format(read_2registers(0x0184)*0.0001, ".2f")),
            "Q2":"".join(format(read_2registers(0x0186)*0.0001, ".2f")),
            "Q3":"".join(format(read_2registers(0x0188)*0.0001, ".2f")),
            "Q":"".join(format(read_2registers(0x0182)*0.0001, ".2f")),
            "S1":"".join(format(read_2registers(0x018C)*0.0001, ".2f")),
            "S2":"".join(format(read_2registers(0x018E)*0.0001, ".2f")),
            "S3":"".join(format(read_2registers(0x0190)*0.0001, ".2f")),
            "S":"".join(format(read_2registers(0x018A)*0.0001, ".2f")),
            "PF1":"".join(format(read_1register(0x0193)*0.01, ".2f")),
            "PF2":"".join(format(read_1register(0x0194)*0.01, ".2f")),
            "PF3":"".join(format(read_1register(0x0195)*0.01, ".2f")),
            "PF":"".join(format(read_1register(0x0192)*0.01, ".2f")),
            "EA":"".join(format(read_2registers(0x0100)*0.01, ".2f")),
            "ER":"".join(format(read_2registers(0x011E)*0.01, ".2f")),
            "F":"".join(format(read_1register(0x0199)*0.01, ".2f"))
            }]}
        attribute_value=json.dumps(data)
        clientMQTT.publish(f"master/{clientID}/writeattributevalue/{attributeWr}/{assetIDwr}", attribute_value)
        time.sleep(10)

except KeyboardInterrupt:
    print("Disconnecting...")
    clientMQTT.disconnect()
    clientMQTT.loop_stop()
