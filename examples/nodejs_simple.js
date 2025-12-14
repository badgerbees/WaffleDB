#!/usr/bin/env node
/**
 * WaffleDB REST API - Simplest Possible Node.js Example
 * Copy-paste this and it just works. No setup, no config.
 */

const http = require('http');

const BASE_URL = 'http://localhost:8080';

async function request(method, path, data) {
  return new Promise((resolve, reject) => {
    const url = new URL(path, BASE_URL);
    const options = {
      method,
      headers: { 'Content-Type': 'application/json' }
    };

    const req = http.request(url, options, (res) => {
      let body = '';
      res.on('data', (chunk) => body += chunk);
      res.on('end', () => {
        try {
          resolve(JSON.parse(body));
        } catch {
          resolve(body);
        }
      });
    });

    req.on('error', reject);
    if (data) req.write(JSON.stringify(data));
    req.end();
  });
}

async function main() {
  console.log('🚀 WaffleDB Node.js Example\n');

  // Add documents (collection auto-creates!)
  console.log('1️⃣  Adding documents...');
  await request('POST', '/collections/docs/add', {
    vectors: [
      { id: 'doc1', vector: Array(384).fill(0.1), metadata: { title: 'Intro' } },
      { id: 'doc2', vector: Array(384).fill(0.2), metadata: { title: 'Advanced' } }
    ]
  });
  console.log('✅ Documents added!\n');

  // Search
  console.log('2️⃣  Searching...');
  const results = await request('POST', '/collections/docs/search', {
    embedding: Array(384).fill(0.15),
    limit: 5
  });
  console.log('✅ Results:', results.results?.length || 0, 'documents found\n');

  // Delete
  console.log('3️⃣  Deleting...');
  await request('POST', '/collections/docs/delete', { ids: ['doc1'] });
  console.log('✅ Done!\n');
}

main().catch(console.error);
