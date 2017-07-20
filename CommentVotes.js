const AWS = require('aws-sdk');

const db = new AWS.DynamoDB.DocumentClient();

exports.handler = (event, context, callback) => {
  console.log(JSON.stringify(event, null, 2));

  const dbOperationsPromisesArr = [];

  event.Records.forEach(record => {
    console.log('DynamoDB Record: %j', record.dynamodb);

    if (record.eventName !== 'MODIFY') return;

    // fetch the TableName from the event ARN of the form:
    // arn:aws:dynamodb:us-east-1:111111111111:table/test/stream/2020-10-10T08:18:22.385
    // see: http://stackoverflow.com/questions/35278881/how-to-get-the-table-name-in-aws-dynamodb-trigger-function
    const TableName = record.eventSourceARN
      .split(':')[5]
      .split('/')[1];

    // Update individual stat if an up/down vote has been cast.
    if (record.dynamodb.OldImage.up.N !== record.dynamodb.NewImage.up.N ||
      record.dynamodb.OldImage.down.N !== record.dynamodb.NewImage.down.N) {
      const id = record.dynamodb.Keys.id.S;

      // Update the post's individual stat on this branch.
      dbOperationsPromisesArr.push(new Promise((resolve, reject) => {
        db.update({
          AttributeUpdates: {
            individual: {
              Action: 'PUT',
              Value: Number(record.dynamodb.NewImage.up.N) - Number(record.dynamodb.NewImage.down.N),
            },
          },
          Key: { id },
          TableName,
        }, (err, data) => {
          if (err) {
            console.log(err);
            return reject(err);
          }

          return resolve();
        });
      }));
    }
  });

  Promise.all(dbOperationsPromisesArr)
    .then(() => callback(null, 'Successfully updated stats!'))
    .catch(err => {
      console.log('Error updating stats: %j', err);
      callback('Error!');
    });
};
