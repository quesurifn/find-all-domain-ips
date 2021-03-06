const cluster = require('cluster');
const numCPUs = 87;
var fs = require('fs');
const readline = require('readline');
const {Resolver} = require('dns');
const resolver = new Resolver();
resolver.setServers(['1.1.1.1', '1.0.0.1']);

if (cluster.isMaster) {
  const files = fs.readdirSync('./data', {encoding: 'utf8'})
  console.log(`Master ${process.pid} is running`);

  // Fork workers.
  for (let i = 0; i < numCPUs; i++) {
    var worker = cluster.fork();
    worker.send({file: files.shift()});

  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`worker ${worker.process.pid} died`);
  });
} else {
 
  let notFound = 0;

  process.on('message', function(msg) {
    console.log('Worker ' + process.pid + ' has started.');
    const file = msg.file
    const readInterface = readline.createInterface({
      input: fs.createReadStream(`./data/${file}`),
      output: null,
      console: false
    });

    let read_array = [];
    readInterface.on('line', function(line) {
      const domain = line.split('\t')[1].split('.').reverse().join('.')
      try {

        resolver.resolve4(domain, (err, addresses) => {
          if(err) {
            notFound++
            //console.error(err)
          }
          if(!err && addresses.length > 0) {
            const string_addr = addresses.join(";")
            read_array.push(`${domain},${string_addr}`)
            if (read_array.length === 1000) {
              fs.appendFile(`./parsed_data/${file}`,read_array.join('\n'), {encoding: 'utf8'}, (err) => {
                console.log(`${Date.now().toLocaleString()} WRITING: ${file} `)
                if(err) {
                  console.log(err)
                }
                read_array.length = 0
              })
            }
          }
        });

      } catch(e) {
        console.log(e)
      }
    });

    readInterface.on('close', () => {
      console.log(`CLOSING ${file} CLOSING ${file} CLOSING`)
      let joined = read_array.join('\n')
      joined += `\n *****NOT FOUND****** ${notFound}`
      fs.appendFile(`./parsed_data/${file}`,joined, {encoding: 'utf8'}, (err) => {
        if(err) {
          console.log(err)
        }
        process.exit(0)
      })
    })
  });

}