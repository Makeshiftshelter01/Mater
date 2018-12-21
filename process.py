from ruri_dao import CrwalingDAO

cd = CrwalingDAO()

result = cd.select('cook')
print(result)