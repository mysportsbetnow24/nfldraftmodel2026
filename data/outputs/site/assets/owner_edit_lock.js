(function () {
  const card = document.querySelector('.scout-card');
  if (!card) return;

  const editableEls = Array.from(card.querySelectorAll('[data-edit-key]'));
  if (!editableEls.length) return;

  const style = document.createElement('style');
  style.textContent = [
    '.owner-lock-banner {',
    '  display: flex;',
    '  align-items: center;',
    '  justify-content: space-between;',
    '  gap: 0.5rem;',
    '  margin: 0.8rem 1rem 0.3rem;',
    '  padding: 0.45rem 0.6rem;',
    '  border: 1px solid #253442;',
    '  background: #eef3f7;',
    '  font-family: Merriweather, serif;',
    '  font-size: 0.82rem;',
    '}',
  ].join('');
  document.head.appendChild(style);

  const banner = document.createElement('div');
  banner.className = 'owner-lock-banner';
  banner.innerHTML = [
    '<span class="owner-lock-status">Scouting card mode: <strong>Read-only (Public)</strong></span>',
    '<span>Editing is restricted to owner/admin workflow.</span>',
  ].join('');

  const controls = card.querySelector('.controls');
  if (controls && controls.parentNode) {
    controls.parentNode.insertBefore(banner, controls);
  } else {
    card.appendChild(banner);
  }

  editableEls.forEach((el) => {
    el.setAttribute('contenteditable', 'false');
  });

  const buttons = Array.from(card.querySelectorAll('.controls .btn'));
  buttons.forEach((btn) => {
    btn.disabled = true;
    btn.style.opacity = '0.55';
    btn.style.pointerEvents = 'none';
  });
})();
