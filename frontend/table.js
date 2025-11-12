document.addEventListener('DOMContentLoaded', () => {
  const form = document.getElementById('etlForm');
  const input = document.getElementById('uploadInput');
  const loading = document.getElementById('loading');
  const output = document.getElementById('output');

  form.addEventListener('submit', ev => {
    ev.preventDefault();
    output.innerHTML = '';
    loading.style.display = 'block';

    let fd = new FormData();
    if (input.files.length) {
      fd.append('inputFile', input.files[0]);
    }

    fetch('http://localhost:5000/run-etl', {
      method: 'POST',
      body: fd,
    })
      .then(res => res.json())
      .then(data => {
        loading.style.display = 'none';
        if (data.success && data.table && data.table.length) {
          renderTable(data.table);
        } else {
          output.innerHTML = `<div style="color:#d02927;font-weight:bold;">Error: ${data.error || 'No data returned.'}</div>`;
        }
      })
      .catch(() => {
        loading.style.display = 'none';
        output.innerHTML =
          '<div style="color:#d02927;font-weight:bold;">Network or server error. Make sure backend is running.</div>';
      });
  });

  function renderTable(rows) {
    let html = '<table><thead><tr>';
    html += Object.keys(rows[0])
      .map(key => `<th>${key}</th>`)
      .join('');
    html += '</tr></thead><tbody>';
    for (let row of rows) {
      html += '<tr>' + Object.values(row).map(val => `<td>${val !== null ? val : ''}</td>`).join('') + '</tr>';
    }
    html += '</tbody></table>';
    output.innerHTML = html;
  }
});
