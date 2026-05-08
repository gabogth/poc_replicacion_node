const http = require('http');
const { execFile } = require('child_process');
const { promisify } = require('util');

const execFileAsync = promisify(execFile);
const PORT = Number(process.env.PORT || 3000);
const KAFKA_BOOTSTRAP = process.env.KAFKA_BOOTSTRAP || 'localhost:9092';
const KAFKA_CONNECT_URL = process.env.KAFKA_CONNECT_URL || 'http://localhost:8083';
const KAFKA_BIN = process.env.KAFKA_BIN || '/opt/kafka/bin';

async function getConnectors() {
  try {
    const response = await fetch(`${KAFKA_CONNECT_URL}/connectors`);
    if (!response.ok) return { error: `Kafka Connect devolvió ${response.status}` };
    const data = await response.json();
    return { items: data };
  } catch (error) {
    return { error: `No se pudo consultar Kafka Connect: ${error.message}` };
  }
}

async function getTopics() {
  try {
    const { stdout } = await execFileAsync(`${KAFKA_BIN}/kafka-topics.sh`, [
      '--bootstrap-server',
      KAFKA_BOOTSTRAP,
      '--list',
    ]);

    const topics = stdout
      .split('\n')
      .map((line) => line.trim())
      .filter(Boolean)
      .sort();

    return { items: topics };
  } catch (error) {
    return { error: `No se pudieron listar tópicos: ${error.message}` };
  }
}

async function getGroupsWithTopics() {
  try {
    const { stdout } = await execFileAsync(`${KAFKA_BIN}/kafka-consumer-groups.sh`, [
      '--bootstrap-server',
      KAFKA_BOOTSTRAP,
      '--all-groups',
      '--describe',
    ]);

    const lines = stdout.split('\n').map((line) => line.trim()).filter(Boolean);
    const rows = lines.filter((line) => !line.startsWith('GROUP') && !line.startsWith('Consumer group'));

    const topicMap = {};
    rows.forEach((line) => {
      const columns = line.split(/\s+/);
      const [group, topic] = columns;
      if (!group || !topic || group === 'Error:') return;
      if (!topicMap[topic]) topicMap[topic] = new Set();
      topicMap[topic].add(group);
    });

    const result = {};
    Object.keys(topicMap)
      .sort()
      .forEach((topic) => {
        result[topic] = Array.from(topicMap[topic]).sort();
      });

    return { items: result };
  } catch (error) {
    return { error: `No se pudieron listar consumer groups: ${error.message}` };
  }
}

function renderHtml({ connectors, topics, groupsByTopic }) {
  const connectorList = connectors.items?.length
    ? connectors.items.map((name) => `<li>${name}</li>`).join('')
    : '<li>No hay conectores</li>';

  const topicRows = topics.items?.length
    ? topics.items
        .map((topic) => {
          const groups = groupsByTopic.items?.[topic] || [];
          const groupsHtml = groups.length ? groups.map((g) => `<code>${g}</code>`).join(', ') : '<em>Sin consumer groups</em>';
          return `<tr><td>${topic}</td><td>${groupsHtml}</td></tr>`;
        })
        .join('')
    : '<tr><td colspan="2">No hay tópicos</td></tr>';

  const errorBlock = [connectors.error, topics.error, groupsByTopic.error]
    .filter(Boolean)
    .map((e) => `<li>${e}</li>`)
    .join('');

  return `<!doctype html>
<html lang="es">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Kafka Dashboard</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 2rem; }
      h1, h2 { margin-bottom: 0.5rem; }
      table { border-collapse: collapse; width: 100%; }
      th, td { border: 1px solid #ccc; padding: 0.6rem; text-align: left; }
      th { background: #f6f6f6; }
      .errors { color: #a40000; }
    </style>
  </head>
  <body>
    <h1>Kafka Dashboard</h1>
    ${errorBlock ? `<ul class="errors">${errorBlock}</ul>` : ''}

    <h2>Conectores</h2>
    <ul>${connectorList}</ul>

    <h2>Tópicos y consumerGroupId</h2>
    <table>
      <thead>
        <tr><th>Tópico</th><th>Consumer Groups</th></tr>
      </thead>
      <tbody>${topicRows}</tbody>
    </table>
  </body>
</html>`;
}

const server = http.createServer(async (req, res) => {
  if (req.url !== '/') {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Not found');
    return;
  }

  const [connectors, topics, groupsByTopic] = await Promise.all([
    getConnectors(),
    getTopics(),
    getGroupsWithTopics(),
  ]);

  const html = renderHtml({ connectors, topics, groupsByTopic });
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(html);
});

server.listen(PORT, () => {
  console.log(`Servidor activo en http://localhost:${PORT}`);
});
