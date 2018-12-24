import model

print(model.used_car.extend_test())
print(model.used_car.find_one())

pipeline = [
    {
        '$match':
            {
                'car_no': '6351728845460078592'
            }
    },
    {
        '$lookup':
            {
                'from': 'users',
                'localField': 'owner',
                'foreignField': '_id',
                'as': 'lowner'
            }
    },
    {
        '$project': {
            'owner': 1,
            'lowner._id': 1
        }
    }
]

for doc in model.used_car.aggregate(pipeline=pipeline):
    print(doc)
