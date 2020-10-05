const cluster = require('cluster');
const numCPUs = 87;
var fs = require('fs');
const readline = require('readline');
const dns = require('dns');
const resolver = new Resolver();
resolver.setServers(['1.1.1.1', '1.0.0.1']);

const files = fs.readdirSync('./data', {encoding: 'utf8'})


if (cluster.isMaster) {
  console.log(`Master ${process.pid} is running`);

  // Fork workers.
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`worker ${worker.process.pid} died`);
  });
} else {

  const file = files.shift()
  const readInterface = readline.createInterface({
    input: fs.createReadStream(`./data/${file}`),
    output: process.stdout,
    console: false
  });

  readInterface.on('line', function(line) {

    const domain = line.split('\t')[1].split('.').reverse().join('.')

    resolver.resolve4(domain, (err, addresses) => {
      if(err) {
        console.error(err)
      }

      const string_addr = addresses.join(";")

      fs.appendFile(`./parsed_data/${file}`,`${domain,string_addr}`, {encoding: 'utf8'}, (err) => {
        if(err) {
          console.log(err)
        }
      });
    });

  });
}