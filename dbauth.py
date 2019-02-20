# provides trasepad login credentials and connection object
# decryption tools
from cryptography.fernet import Fernet
import hashlib
# postgreSQL interface
import psycopg2
import psycopg2.extras

def dbauth():
    # returns encrypted login credentials
    a = b'UYNvmClsL49I_RymoPTiwQrPwZaRo3Z9Mj43IEqvpYQ=' 
    b = b'gAAAAABbTzwKO8c27BELWBFimag05sVqh4t7R0Fyl9wb_rAgU1t0oBB2Vk7cRq5pQHCdf4lqD5Z7wOrz7uADLvKzCMIgzYFLg1_MoQrV_TS0pS268t6ztE59z4UXV5ProRGEL_TQN096YpfbaekTkWuJPLXI0BGxYn7TmD3Zt9DdfuM-3gaXRro='
    return (a,b)   

def dbconn(): 
    # logs in to trasepad database and returns a connection object  
    (a, b) = dbauth()
    db = psycopg2.connect(Fernet(a).decrypt(b).decode('utf-8')) 
    return db  