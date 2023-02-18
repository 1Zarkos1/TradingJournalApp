from tinkoff.invest.schemas import MoneyValue

def extract_money_amount(moneyObj: MoneyValue) -> float:
    return round(moneyObj.units + moneyObj.nano*0.000000001, 2)
