#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pontificia Universidade Catolica de Minas Gerais
Instituto de Ciencias Exatas e Informatica
Engenharia de Software
Desenvolvimento de Aplicacoes Moveis e Distribuidas
Marco Tulio Zuquim
Pedro Henrique Araujo

Broker application (daemon + concurrent stock trade offer maker)
"""
import pika  # Must use version=0.13.1
import logging
import threading

from time import strptime
from sys import argv, exit
from json import dumps, loads
from ipaddress import ip_address
from os import getpid, kill, remove
from os.path import basename, exists
from getopt import getopt, GetoptError

# Logging setup
format = "%(asctime)s  %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

# Global exchange declaration
_exchange = 'BOLSADEVALORES'

# Usage help text
_usage = (
    f'Usage:\n{basename(__file__)} -a <mq_address> [-p <mq_port>]'
    f' -o <b[uy]|s[ell]|i[nfo]> [-c <stock_code> -v <target_value>]'
    f' [-d <date_time>]'
    f'\n\t-a,  --address=        Message Queue server IP address'
    f'\n\t-p,  --port=           Message Queue server port (default: 5672)'
    f'\n\t-o,  --operation=      Stock operation: b[uy] / s[ell] / i[nfo]'
    f'\n\t-c,  --code=           Stock code (egg.: AAPL, GOOG)'
    f'\n\t-s,  --shares=         Number of shares (int value)'
    f'\n\t-v,  --target-value=   Target value for stock (float value)'
    f'\n\t-d,  --date-time=      Date time. Format: dd/mm/YYYY HH:MM\n'
)


def main(arguments):
    # Argument parsing
    try:
        opts, args = getopt(
            arguments,
            "ha:p:o:c:s:v:d:",
            [
                "address=", "port=",
                "operation=", "code=", "shares", "target-value=", "date-time="
            ]
        )
    except GetoptError:
        logging.error(
            f'Missing arguments. Arguments used: {arguments}\n{_usage}'
        )
        exit(1)
    else:
        for opt, arg in opts:
            if opt == '-h':
                logging.info(_usage)
                exit(0)
            elif opt in ("-a", "--address"):
                address = arg
            elif opt in ("-p", "--port"):
                port = arg
            elif opt in ("-o", "--operation"):
                operation = arg
            elif opt in ("-c", "--code"):
                code = arg
            elif opt in ("-s", "--shares"):
                shares = arg
            elif opt in ("-v", "--target-value"):
                target_value = arg
            elif opt in ("-d", "--date-time"):
                date_time = arg

       # Checking arguments
        try:
            ip_address(address)
        except ValueError:
            logging.error(
                f'Invalid IP address ({address}). Enter a valid IP address.'
            )
            exit(1)
        except UnboundLocalError:
            logging.error(
                f'Missing IP address. Enter a valid IP address.\n{_usage}'
            )
            exit(1)
        else:
            host = address
        try:
            if not port.isdecimal():
                logging.error(
                    f'Invalid port number ({port}).'
                    f' Enter a valid port number.'
                )
                exit(1)
        except UnboundLocalError:
            port = 5672
        try:
            if not any(
                    substring in operation for substring in [
                        'b', 's', 'buy', 'sell', 'i', 'info'
                    ]
            ):
                raise NameError
        except NameError:
            logging.error(
                f'Invalid operation.'
                f' Use one of these options:'
                f' [ b, buy, s, sell, i, info ]\n{_usage}'
            )
            exit(1)
        try:
            code.isalnum()
        except UnboundLocalError:
            logging.error(f'Invalid stock code.\n{_usage}')
            exit(1)
        if 'i' not in operation:
            try:
                if not shares.isdecimal():
                    raise UnboundLocalError
            except UnboundLocalError:
                logging.error(f'Invalid number of shares.\n{_usage}')
                exit(1)
            try:
                value = float(target_value)
            except ValueError:
                logging.error(f'Invalid target value ({target_value}).\n{_usage}')
                exit(1)
            except UnboundLocalError:
                logging.error(f'Invalid target value.\n{_usage}')
                exit(1)
        else:
            try:
                dt = strptime(date_time, '%d/%m/%Y %H:%M')
            except ValueError as e:
                logging.error(e)
                exit(1)
            except UnboundLocalError:
                logging.error(f'Invalid date time value.\n{_usage}')
                exit(1)

    # Topic routing key setup
    if 'b' in operation:
        routing_key = f'compra.{code}'
    elif 's' in operation:
        routing_key = f'venda.{code}'
    else:
        routing_key = f'info.{code}'

    # Declaring global vars
    global t

    # Assigning value to global vars
    t = 0

    # Biding keys to be consumed
    biding_keys = ['transacao.*', 'info.*']

    # Consumer thread execution
    def start_consumer(thread):
        global t
        t += 1
        logging.info(f"Thread {t}: starting PID={getpid()}")
        thread.start()
        thread.join()
        logging.info(f"Thread {t}: finishing PID={getpid()}")
        remove(broker_pid)

    # Configuration and execution of consumer daemon
    def consumer(parameters, queue='BOLSA', exchange=_exchange):
        con = pika.BlockingConnection(parameters=parameters)
        channel = con.channel()
        channel.exchange_declare(exchange=exchange, exchange_type='topic')
        result = channel.queue_declare(queue)
        q = result.method.queue
        for biding_key in biding_keys:
            channel.queue_bind(
                exchange=exchange, queue=q, routing_key=biding_key
            )
        channel.basic_consume(thread_callback, queue=q, no_ack=True)
        channel.start_consuming()

    # Configuration and execution of stock trade offer
    def make_offer(parameters, message, exchange=_exchange, r_key=routing_key):
        con = pika.BlockingConnection(parameters=parameters)
        channel = con.channel()
        channel.exchange_declare(exchange=exchange, exchange_type='topic')
        channel.basic_publish(exchange=exchange, routing_key=r_key, body=dumps(message))
        logging.info(f'Sent: {r_key} {dumps(message)}')
        con.close()

    # Thread for simple printer for callback's body message
    def printer(method, body):
        logging.info(f"Received: {method.routing_key} {body.decode()}")

    # Configuration and execution of callback printer thread
    def thread_callback(ch, method, properties, body):
        global t
        t += 1
        i = t
        callback = threading.Thread(
            target=printer, args=(method, body), daemon=True
        )
        logging.info(f"Thread {i}: starting\tPID={getpid()}")
        callback.start()
        callback.join()
        logging.info(f"Thread {i}: finishing\tPID={getpid()}")

    # Message format: stocks_quantity, target_value, broker_id
    if 'i' not in operation:
        message = {"quant": shares, "value": value, "broker": getpid()}
    else:
        message = {"datetime": f'"{date_time}"'}

    # Configuration and execution of stock trade offer maker thread
    parameters = pika.ConnectionParameters(host=host, port=port)
    cthread = threading.Thread(target=consumer, args=(parameters,), daemon=True)
    pthread = threading.Thread(
        target=make_offer, args=(parameters, message), daemon=True
    )
    i = t
    logging.info(f"Thread {i}: starting\tPID={getpid()}")
    pthread.start()
    pthread.join()
    logging.info(f"Thread {i}: finishing\tPID={getpid()}")

    """
    Checks if there's a file containing a PID for the consumer daemon.
    If it exists, checks if the PID is currently running.
    If the daemon is already running, just send the stock trade offer and exits.
    Else, it sends the stock trade offer,
    then starts the daemon and creates the current PID file.
    The daemon will keep running until KeyboardInterrupt (Ctrl+C).
    """
    broker_pid = f'/tmp/{basename(__file__).strip(".py")}.pid'
    if exists(broker_pid):
        try:
            with open(broker_pid, 'r') as f:
                _pid = int(f.read())
        except ValueError:
            with open(broker_pid, 'w') as p:
                p.write(str(getpid()))
            start_consumer(cthread)
        else:
            try:
                kill(_pid, 0)
            except OSError:
                with open(broker_pid, 'w') as p:
                    p.write(str(getpid()))
                start_consumer(cthread)
            else:
                logging.info(
                    f'{basename(__file__)} already running.\tPID={_pid}'
                )
    else:
        with open(broker_pid, 'w') as p:
            p.write(str(getpid()))
        start_consumer(cthread)


if __name__ == "__main__":
    # Run consumer daemon until Ctrl+C is pressed
    try:
        main(argv[1:])
    except KeyboardInterrupt:
        logging.warning('Keyboard Interrupted (Ctrl+C)')
        broker_pid = f'/tmp/{basename(__file__).strip(".py")}.pid'
        if exists(broker_pid):
            remove(broker_pid)
        exit(0)
