
ob = {
    'immobilientypDaten': 
        {
        'immoTyp': 0,
        'immoRentType': 0
        },
}
ausstattung = []
ausstattung.append( {'name': 'test'})
ausstattung.append( {'name': 'test2'})
ob['ausstattungDaten'] = ausstattung

print(ob)