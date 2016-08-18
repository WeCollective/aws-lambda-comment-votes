var AWS = require('aws-sdk');

var db = new AWS.DynamoDB.DocumentClient();

exports.handler = function(event, context, callback) {
  console.log(JSON.stringify(event, null, 2));
  var promises = [];  // db operations wrapped in promises and pushed to this array
  event.Records.forEach(function(record) {
    console.log('DynamoDB Record: %j', record.dynamodb);

    if(record.eventName != 'MODIFY') { return; }

    // fetch the dbTable from the event ARN of the form:
    // arn:aws:dynamodb:us-east-1:111111111111:table/test/stream/2020-10-10T08:18:22.385
    // see: http://stackoverflow.com/questions/35278881/how-to-get-the-table-name-in-aws-dynamodb-trigger-function
    var dbTable = record.eventSourceARN.split(':')[5].split('/')[1];

    // Update individual stat if an up/down vote has been cast
    if(record.dynamodb.OldImage.up.N != record.dynamodb.NewImage.up.N ||
       record.dynamodb.OldImage.down.N != record.dynamodb.NewImage.down.N) {
      // update the post's individual stat on this branch
      promises.push(new Promise(function(resolve, reject) {
        db.update({
          TableName: dbTable,
          Key: {
            id: record.dynamodb.Keys.id.S
          },
          AttributeUpdates: {
            individual: {
              Action: 'PUT',
              Value: Number(record.dynamodb.NewImage.up.N) - Number(record.dynamodb.NewImage.down.N)
            }
          }
        }, function(err, data) {
          if(err) {
            console.log(err);
            return reject(err);
          }
          resolve();
        });
      }));
    }
  });

  // resolve all updates
  Promise.all(promises).then(function() {
    callback(null, "Successfully updated stats!");
  }, function(err) {
    console.log("Error updating stats: %j", err);
    callback("Error!");
  });
};
