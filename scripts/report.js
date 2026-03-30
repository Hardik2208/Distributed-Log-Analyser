/**
 * Pulls current metrics from server and prints a clean terminal report
 * Usage:
 *   node scripts/report.js            — all services
 *   node scripts/report.js auth       — specific service
 *   node scripts/report.js dlq        — DLQ analytics only
 */

const SERVER  = 'http://localhost:3000';
const service = process.argv[2];

async function get(path) {
  const r = await fetch(`${SERVER}${path}`);
  return r.json();
}

function bar(value, max, width = 20) {
  const filled = Math.round((value / max) * width);
  return '█'.repeat(filled) + '░'.repeat(width - filled);
}

async function printService(svc) {
  const data = await get(`/metrics?service=${svc}`);
  if (!data.windows?.length) { console.log(`  No data for service: ${svc}`); return; }

  console.log(`\n${'═'.repeat(70)}`);
  console.log(` Service: ${svc.toUpperCase()}`);
  console.log(`${'═'.repeat(70)}`);

  let totalOrig = 0, totalRetry = 0, totalErr = 0, totalReq = 0;
  let sumPipeline = 0, sumIngestion = 0;

  for (const w of data.windows) {
    totalReq     += w.request_count         || 0;
    totalOrig    += w.original_count        || 0;
    totalRetry   += w.retry_count           || 0;
    totalErr     += w.error_count           || 0;
    sumPipeline  += (w.avg_pipeline_latency  || 0) * (w.request_count || 0);
    sumIngestion += (w.avg_ingestion_latency || 0) * (w.request_count || 0);
  }

  const amp      = totalOrig > 0 ? (totalRetry / totalOrig).toFixed(2) : '0.00';
  const failRate = totalReq  > 0 ? ((totalErr / totalReq) * 100).toFixed(1) : '0.0';
  const avgPl    = totalReq  > 0 ? Math.round(sumPipeline  / totalReq) : 0;
  const avgIng   = totalReq  > 0 ? Math.round(sumIngestion / totalReq) : 0;
  const ampNum   = parseFloat(amp);

  const ampIndicator = ampNum > 3 ? '🔴' : ampNum > 1.5 ? '🟡' : '🟢';

  console.log(`  Total requests:        ${totalReq.toLocaleString()}`);
  console.log(`  Original messages:     ${totalOrig.toLocaleString()}`);
  console.log(`  Retried messages:      ${totalRetry.toLocaleString()}`);
  console.log(`  Error count:           ${totalErr.toLocaleString()} (${failRate}%)`);
  console.log(`  Retry amplification:   ${ampIndicator} ${amp}x  ${bar(Math.min(ampNum, 5), 5)}`);
  console.log(`  Avg pipeline latency:  ${avgPl}ms`);
  console.log(`  Avg ingestion latency: ${avgIng}ms`);

  if (data.windows.length > 1) {
    console.log(`\n  Per-window breakdown (${data.windows.length} windows):`);
    console.log('  ' + 'Window'.padEnd(16) + 'Requests'.padStart(10) + 'Amp'.padStart(8) + 'Fail%'.padStart(8) + 'Pipeline ms'.padStart(14));
    console.log('  ' + '─'.repeat(56));
    for (const w of data.windows) {
      const wAmp  = w.original_count > 0 ? (w.retry_count / w.original_count).toFixed(2) : '0.00';
      const wFail = w.request_count  > 0 ? ((w.error_count / w.request_count) * 100).toFixed(1) : '0.0';
      const d = new Date(parseInt(w.window));
      const ts = `${d.getHours()}:${String(d.getMinutes()).padStart(2,'0')}`;
      console.log(
        '  ',
        ts.padEnd(16),
        String(w.request_count).padStart(10),
        (wAmp + 'x').padStart(8),
        (wFail + '%').padStart(8),
        String(Math.round(w.avg_pipeline_latency)).padStart(14),
      );
    }
  }
}

async function printDLQ() {
  const dlq = await get('/metrics/dlq');
  console.log(`\n${'═'.repeat(70)}`);
  console.log(' DLQ Analytics');
  console.log(`${'═'.repeat(70)}`);
  console.log(`  Total entries:             ${dlq.count}`);
  console.log(`  Avg retries before DLQ:    ${dlq.avg_retries_before_dlq}`);
  console.log(`  Avg time in pipeline (ms): ${dlq.avg_time_in_pipeline_ms}`);
  console.log(`  Failure reasons:`);
  for (const [reason, count] of Object.entries(dlq.by_reason || {})) {
    console.log(`    ${reason.padEnd(28)} ${count}`);
  }
  if (dlq.recent?.length) {
    console.log(`\n  Recent entries (last ${dlq.recent.length}):`);
    for (const e of dlq.recent.slice(-5)) {
      console.log(`    ${e.id?.slice(0,8)}...  retries=${e.retry_count}  reason=${e.reason}  total=${e.total_time_in_pipeline}ms`);
    }
  }
}

async function run() {
  try {
    if (service === 'dlq') {
      await printDLQ();
    } else if (service) {
      await printService(service);
      await printDLQ();
    } else {
      for (const svc of ['auth', 'payment', 'order']) {
        await printService(svc);
      }
      await printDLQ();
    }
    console.log('');
  } catch (err) {
    console.error('❌ Report failed:', err.message);
    console.error('   Is the server running? → node server.js');
  }
}

run();