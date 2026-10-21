(() => {
  'use strict';

  const services = Object.freeze({
    quant: ['QuantStack 量子栈', 'quant', '/'],
    vibe: ['AI 投研', 'vibe-trading', '/agent'],
    stock: ['行情选股', 'stock-screener', '/'],
    nautilus: ['策略实施', 'nautilus', '/'],
    learn: ['学习中心', 'learn', '/'],
    ragflow: ['RAGFlow', 'ragflow', '/'],
    raggraph: ['RagGraph', 'raggraph', '/'],
  });

  const service = window.location.pathname.split('/').filter(Boolean).at(-1);
  const selected = services[service];
  const form = document.querySelector('#login-form');
  const status = document.querySelector('#form-status');
  const button = document.querySelector('#submit-button');
  const safeErrors = new Set([
    '尝试次数过多，请稍后再试。',
    '账号或密码不正确。',
    '登录目标校验失败，请联系管理员。',
  ]);

  if (!selected) {
    form.hidden = true;
    status.textContent = '访问目标无效。';
    return;
  }
  document.querySelector('#service-name').textContent = selected[0];
  const parentDomain = window.location.hostname.replace(/^auth\./, '');
  const port = window.location.port ? `:${window.location.port}` : '';
  const expectedRedirect = `https://${selected[1]}.${parentDomain}${port}${selected[2]}`;
  const expectedConsume = `${new URL(expectedRedirect).origin}/platform-auth/consume`;

  const basicValue = (username, password) => {
    const bytes = new TextEncoder().encode(`${username}:${password}`);
    let binary = '';
    bytes.forEach((byte) => { binary += String.fromCharCode(byte); });
    return `Basic ${window.btoa(binary)}`;
  };

  const submitHandoff = (payload) => {
    if (payload.redirect !== expectedRedirect || payload.consume !== expectedConsume
        || typeof payload.ticket !== 'string'
        || !/^v3\.handoff\.[A-Za-z0-9_-]+\.\d+\.\d+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(payload.ticket)) {
      throw new Error('登录目标校验失败，请联系管理员。');
    }
    form.password.value = '';
    const handoff = document.createElement('form');
    handoff.method = 'POST';
    handoff.action = expectedConsume;
    const ticket = document.createElement('input');
    ticket.type = 'hidden';
    ticket.name = 'ticket';
    ticket.value = payload.ticket;
    handoff.append(ticket);
    document.body.append(handoff);
    handoff.submit();
  };

  const recoverForm = (message = '') => {
    status.textContent = message;
    button.disabled = false;
    button.textContent = '登录并继续';
  };

  const resumeExistingSession = async () => {
    button.disabled = true;
    status.textContent = '正在检查现有登录…';
    try {
      const response = await window.fetch(`/handoff/${service}`, {
        method: 'POST',
        credentials: 'same-origin',
        cache: 'no-store',
        headers: { Accept: 'application/json' },
      });
      if (response.status === 401) {
        recoverForm();
        return;
      }
      if (!response.ok) throw new Error();
      submitHandoff(await response.json());
    } catch {
      recoverForm('登录状态检查失败，请重新登录。');
    }
  };

  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    status.textContent = '';
    button.disabled = true;
    button.textContent = '正在验证…';

    try {
      const response = await window.fetch(`/session/${service}`, {
        method: 'POST',
        credentials: 'same-origin',
        cache: 'no-store',
        headers: {
          Authorization: basicValue(form.username.value, form.password.value),
          Accept: 'application/json',
        },
      });
      if (!response.ok) {
        throw new Error(response.status === 429 ? '尝试次数过多，请稍后再试。' : '账号或密码不正确。');
      }
      submitHandoff(await response.json());
    } catch (error) {
      form.password.value = '';
      recoverForm(error instanceof Error && safeErrors.has(error.message)
        ? error.message
        : '登录失败，请重试。');
      form.password.focus();
    }
  });

  resumeExistingSession();
})();
