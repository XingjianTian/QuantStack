(() => {
  const root = document.documentElement;
  const scenes = Array.from(document.querySelectorAll('[data-scene-button]'));
  const panels = Array.from(document.querySelectorAll('[data-scene-panel]'));
  let active = scenes.findIndex((button) => button.getAttribute('aria-selected') === 'true');
  let timer;

  const show = (index, userInitiated = false) => {
    active = (index + scenes.length) % scenes.length;
    const id = scenes[active]?.dataset.sceneButton;
    if (!id) return;
    root.dataset.scene = id;
    scenes.forEach((button, buttonIndex) => {
      const selected = buttonIndex === active;
      button.setAttribute('aria-selected', String(selected));
      button.tabIndex = selected ? 0 : -1;
    });
    panels.forEach((panel) => {
      panel.hidden = panel.dataset.scenePanel !== id;
    });
    if (userInitiated) restart();
  };

  const restart = () => {
    window.clearInterval(timer);
    if (window.matchMedia('(prefers-reduced-motion: reduce)').matches || scenes.length < 2) return;
    timer = window.setInterval(() => show(active + 1), 4200);
  };

  scenes.forEach((button, index) => {
    button.addEventListener('click', () => show(index, true));
    button.addEventListener('keydown', (event) => {
      if (event.key === 'ArrowRight' || event.key === 'ArrowDown') {
        event.preventDefault();
        show(index + 1, true);
        scenes[(index + 1) % scenes.length]?.focus();
      }
      if (event.key === 'ArrowLeft' || event.key === 'ArrowUp') {
        event.preventDefault();
        show(index - 1, true);
        scenes[(index - 1 + scenes.length) % scenes.length]?.focus();
      }
    });
  });

  document.querySelector('[data-menu-button]')?.addEventListener('click', () => {
    const open = root.dataset.menuOpen !== 'true';
    root.dataset.menuOpen = String(open);
    document.querySelector('[data-menu-button]')?.setAttribute('aria-expanded', String(open));
  });

  show(Math.max(active, 0));
  restart();
})();
