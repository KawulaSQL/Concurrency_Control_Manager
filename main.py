# import file
from TwoPhaseLocking import *

tpl = TwoPhaseLocking()

def transaction_1():
    tpl.acquire_lock(1, "Resource1", "exclusive")
    tpl.acquire_lock(1, "Resource2", "shared")
    tpl.commit_transaction(1)

def transaction_2():
    tpl.acquire_lock(2, "Resource1", "shared")
    tpl.acquire_lock(2, "Resource3", "exclusive")
    tpl.commit_transaction(2)

t1 = threading.Thread(target=transaction_1)
t2 = threading.Thread(target=transaction_2)

t1.start()
t2.start()

t1.join()
t2.join()