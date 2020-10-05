const cluster = require('cluster');
const numCPUs = 87;
var fs = require('fs');
const readline = require('readline');
const {Resolver} = require('dns');
const resolver = new Resolver();
resolver.setServers(['1.1.1.1', '1.0.0.1']);

if (cluster.isMaster) {
    const files = fs.readdirSync('./data', {encoding: 'utf8'})
    console.log(files)
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

  const read_array = [];
  readInterface.on('line', function(line) {
    const domain = line.split('\t')[1].split('.').reverse().join('.')
    resolver.resolve4(domain, (err, addresses) => {
      if(err) {
        console.error(err)
      }

      if(!err && addresses.length > 0) {
        const string_addr = addresses.join(";")
        read_array.push(`${domain,string_addr}`)
        if (read_array.length === 1000) {
          fs.appendFile(`./parsed_data/${file}`,read_array.join('\n'), {encoding: 'utf8'}, (err) => {
            console.log(`${Date.now().toLocaleString()} WRITING: ${file} `)
            if(err) {
              console.log(err)
            }
          })
        }
      }

    });
  });

  readInterface.on('close', () => {
    fs.appendFile(`./parsed_data/${file}`,read_array.join('\n'), {encoding: 'utf8'}, (err) => {
      if(err) {
        console.log(err)
      }
    })
  })
}