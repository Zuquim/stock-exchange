#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pontificia Universidade Catolica de Minas Gerais
Instituto de Ciencias Exatas e Informatica
Engenharia de Software
Desenvolvimento de Aplicacoes Moveis e Distribuidas
Marco Tulio Zuquim
Pedro Henrique Araujo

Stock Exchange application (daemon)
"""
import pika  # Must use version=0.13.1
import logging
import threading
import sqlite3 as sql

from sys import argv, exit
from json import dumps, loads
from time import time, strftime
from ipaddress import ip_address
from os import getpid, kill, remove
from os.path import basename, exists
from getopt import getopt, GetoptError

# Logging setup
_format = "%(asctime)s: %(message)s"
logging.basicConfig(format=_format, level=logging.INFO, datefmt="%H:%M:%S")

# Global exchange declaration
_exchange = 'BOLSADEVALORES'

# Global thread number counter
t = 0

# Usage help text
_usage = (
    f'Usage:\n{basename(__file__)} -a <mq_address> [-p <mq_port>]'
    f'\n\t-a,  --address=ADDRESS  Message Queue server IP address'
    f'\n\t-p,  --port=PORT        Message Queue server port (default: 5672)\n'
)


# Thread to publish a transaction
def communication_thread(channel, stock, message, topic):
    routing_key = f'{topic}.{stock.lower()}'
    channel.basic_publish(
        exchange=_exchange, routing_key=routing_key, body=dumps(message)
    )
    logging.info(f'Sent: {routing_key} {dumps(message)}')


# Configuration and execution of thread to publish transaction
def communicate_transaction(channel, stock, quant, value):
    # Setting up vars
    global t
    t += 1
    i = t

    channel.exchange_declare(exchange=_exchange, exchange_type='topic')

    # Message format: stocks_quantity, target_value, broker_id
    message = {"quant": quant, "value": value}

    pthread = threading.Thread(
        target=communication_thread,
        args=(channel, stock, message, 'transacao'),
        daemon=True
    )
    logging.info(f"Thread {i}: starting\tPID={getpid()}")
    pthread.start()
    pthread.join()
    logging.info(f"Thread {i}: finishing\tPID={getpid()}")


# Query info from certain date time
def info_from_datetime(dt, stock):
    try:
        db = sql.connect(_db)
        c = db.cursor()
        c.execute(
            f"select operation, quantity, value from offers"
            f" where stock = '{stock.upper()}'"
            f" and date_time like '{dt}%'"
        )
    except sql.Error as e:
        logging.error(e)
        return None
    else:
        output = c.fetchall()
        c.close()
        db.close()
        return output


# Info reporter
def info(channel, method, body):
    # Setting up vars
    global t
    t += 1
    i = t
    stock = method.routing_key.split('.')[1]
    decoded = loads(body.decode())
    # FIXME: for some bizarre reason, the JSON object is being both \
    #        normally read AND being read as list instead of JSON... WTF?!
    dt = decoded["datetime"]
    # logging.info(dt)

    # Query DB
    json_list = []
    result = info_from_datetime(dt, stock)
    for item in result:
        json_list.append(
            {
                "code": stock,
                "operation": item[0],
                "quantity": item[1],
                "value": item[2]
            }
        )

    channel.exchange_declare(exchange=_exchange, exchange_type='topic')

    # Message format: stocks_quantity, target_value
    message = json_list

    pthread = threading.Thread(
        target=communication_thread,
        args=(channel, stock, message, 'info'),
        daemon=True
    )
    logging.info(f"Thread {i}: starting\tPID={getpid()}")
    pthread.start()
    pthread.join()
    logging.info(f"Thread {i}: finishing\tPID={getpid()}")


# Creating SQLite DB, in case it doesn't exist
_db = 'stock_exchange.sqlite'
if not exists(_db):
    try:
        db = sql.connect(_db)
        c = db.cursor()
        c.execute(
            "create table offers("
            "epoch real not null constraint offer_pk primary key, "
            "date_time text not null,"
            "stock text not null, "
            "operation text not null, "
            "value real not null, "
            "quantity int not null, "
            "broker text not null"
            ")"
        )
    except sql.Error as e:
        logging.error(e)
        exit(1)
    else:
        c.close()
        db.commit()
        db.close()


# Register stock trade offer in DB
def register(channel, method, body):
    logging.info(f"Received: {method.routing_key} {body.decode()}")

    # Setting up vars
    msg = loads(body.decode())
    oper = method.routing_key.split('.')[0].lower()
    stock = method.routing_key.split('.')[1].upper()
    epoch = time()
    dt = strftime('%d/%m/%Y %H:%M:%S')

    # Inserting new stock trade offer in DB
    try:
        db = sql.connect(_db)
        c = db.cursor()
        c.execute(
            f"insert into offers ("
            f"epoch, date_time, stock, operation, value, quantity, broker"
            f") values ("
            f"{epoch}, "
            f"{dt}, "
            f"'{stock}', "
            f"'{oper}', "
            f"{msg['value']}, "
            f"{msg['quant']}, "
            f"{msg['broker']}"
            f")"
        )
    except sql.Error as e:
        logging.error(e)
        exit(1)
    else:
        c.close()
        db.commit()
        db.close()
        logging.info(f"Registered '{oper}' offer for {stock}")
    offer_analyst(channel, epoch, stock, oper, msg["value"], msg["quant"])


# Update stocks in DB when transactions occur
def update_stocks(cursor, oper, epoch, stock_quant=0):
    if oper == 'del':
        logging.info(f'Deleting stock offer (epoch={epoch})')
        try:
            cursor.execute(f"delete from offers where epoch = {epoch}")
        except sql.Error as e:
            logging.error(e)
            return False
        else:
            return True
    elif oper == 'upd':
        logging.info(f'Updating stock offer (epoch={epoch})')
        try:
            cursor.execute(
                f"update offers "
                f" set quantity = {stock_quant}"
                f" where epoch = {epoch}"
            )
        except sql.Error as e:
            logging.error(e)
            return False
        else:
            return True
    else:
        logging.error(
            f'Invalid operation ({oper}). Must be "del" or "upd".'
        )
        return False


# Analyse transaction offers and calls for DB update and transaction publish
def offer_analyst(channel, epoch, stock, oper, value, quant):
    logging.info(f"Analysing offers")

    # Setting up vars
    _quant = int(quant)
    field = 'epoch'
    ord = 'ASC'
    if oper == 'compra':
        cmp = '<='
    else:
        cmp = '>='

    # Querying DB for possible transactions (prioritizing offer time order)
    try:
        db = sql.connect(_db)
        c = db.cursor()
        c.execute(
            f"select epoch, value, quantity, operation"
            f" from offers"
            f" where stock = '{stock}'"
            f" and operation != '{oper}'"
            f" and value {cmp} {value}"
            f" order by {field} {ord}"
        )
    except sql.Error as e:
        logging.error(e)
    else:
        out = c.fetchall()
        for offer in out:
            stock_epoch = offer[0]
            stock_value = offer[1]
            stock_quant = offer[2]
            stock_opera = offer[3]

            if stock_quant == _quant:

                # Making sure to display the transaction value
                if stock_value <= value:
                    paid = stock_value
                else:
                    paid = value

                # Calling DB updater
                update_stocks(c, 'del', stock_epoch)
                update_stocks(c, 'del', epoch)
                logging.info(
                    f'Quantity match. Traded shares: {_quant}'
                    f' ({stock_opera}: ${stock_value};'
                    f' {oper}: ${value})'
                )

                # Calling transaction publisher
                communicate_transaction(channel, stock, _quant, paid)
                return True
            elif stock_quant < _quant:
                _quant -= stock_quant

                # Making sure to display the transaction value
                if stock_value <= value:
                    paid = stock_value
                else:
                    paid = value

                # Calling DB updater
                update_stocks(c, 'del', stock_epoch)
                logging.info(
                    f'Traded {stock_quant} shares ('
                    f'{stock_opera}: ${stock_value}).'
                    f' Remaining shares to trade: {_quant} ({oper}: ${value})'
                )

                # Calling transaction publisher
                communicate_transaction(channel, stock, stock_quant, paid)
            elif stock_quant > _quant:
                stock_quant -= _quant

                # Making sure to display the transaction value
                if stock_value <= value:
                    paid = stock_value
                else:
                    paid = value

                # Calling DB updater
                update_stocks(c, 'upd', stock_epoch, stock_quant)
                update_stocks(c, 'del', epoch)
                logging.info(
                    f'Traded {_quant} shares ({oper}: ${value}).'
                    f' Remaining shares from previously offered stock:'
                    f' {stock_quant} ({stock_opera}: ${stock_value})'
                )

                # Calling transaction publisher
                communicate_transaction(channel, stock, _quant, paid)
                return True
    finally:
        c.close()
        db.commit()
        db.close()
        logging.info(f"Done analysis")


def main(arguments):
    # Argument parsing
    try:
        opts, args = getopt(arguments, "ha:p:", ["address=", "port="])
    except GetoptError:
        logging.error(_usage)
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

        # Checking arguments
        try:
            ip_address(address)
        except ValueError:
            logging.error(
                f'\nInvalid IP address ({address}). Enter a valid IP address.\n'
            )
            exit(1)
        else:
            host = address
        try:
            if not port.isdecimal():
                logging.error(
                    f'\nInvalid port number ({port}).'
                    f' Enter a valid port number.\n'
                )
                exit(1)
        except UnboundLocalError:
            port = 5672

    # Global thread number counter
    global t

    # Biding keys to be consumed
    biding_keys = ['compra.*', 'venda.*', 'info.*']

    # Consumer daemon
    def consumer(parameters, queue='BROKER', exchange=_exchange):
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

    # Callback switch
    def switch(channel, method, body):
        if 'info' not in method.routing_key:
            register(channel, method, body)
        else:
            info(channel, method, body)

    # Configuration and execution of callback thread to register trade offer
    def thread_callback(channel, method, properties, body):
        global t
        t += 1
        i = t
        callback = threading.Thread(
            target=switch, args=(channel, method, body), daemon=True
        )
        logging.info(f"Thread {i}: starting")
        callback.start()
        callback.join()
        logging.info(f"Thread {i}: finishing")

    # Configuration and execution of consumer thread
    parameters = pika.ConnectionParameters(host=host, port=port)
    consumer = threading.Thread(
        target=consumer, args=(parameters,), daemon=True
    )
    logging.info(f"Thread {t}: starting PID={getpid()}")
    consumer.start()
    consumer.join()
    logging.info(f"Thread {t}: finishing PID={getpid()}")
    remove(_stock_exchange_pid)


if __name__ == "__main__":
    """
    Main caller.
    Checks if there's a file containing a PID for the consumer daemon.
    If it exists, checks if the PID is currently running.
    If the daemon is already running, exits with code 1.
    Else, it starts the daemon and creates the current PID file.
    The daemon will keep running until KeyboardInterrupt (Ctrl+C).
    """
    _stock_exchange_pid = f'/tmp/{basename(__file__).strip(".py")}.pid'
    if exists(_stock_exchange_pid):
        try:
            with open(_stock_exchange_pid, 'r') as f:
                _pid = int(f.read())
        except ValueError:
            with open(_stock_exchange_pid, 'w') as p:
                p.write(str(getpid()))
        else:
            try:
                kill(_pid, 0)
            except OSError:
                with open(_stock_exchange_pid, 'w') as p:
                    p.write(str(getpid()))
            else:
                logging.error(
                    f'{basename(__file__)} already running!\tPID={_pid}'
                )
                exit(1)
    else:
        with open(_stock_exchange_pid, 'w') as p:
            p.write(str(getpid()))

    # Run damon until Ctrl+C is pressed
    try:
        main(argv[1:])
    except KeyboardInterrupt:
        if exists(_stock_exchange_pid):
            remove(_stock_exchange_pid)
        logging.warning('Keyboard Interrupted (Ctrl+C)')
        exit(0)
