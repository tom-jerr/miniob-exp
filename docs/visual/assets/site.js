(() => {
  const root = document.documentElement;
  const store = {
    get(key) {
      try { return localStorage.getItem(key); } catch (e) { return null; }
    },
    set(key, value) {
      try { localStorage.setItem(key, value); } catch (e) { /* ignore */ }
    },
  };

  // 1. Theme toggle
  const themeBtn = document.getElementById("themeBtn");
  if (themeBtn) {
    const syncTheme = () => {
      themeBtn.textContent = root.dataset.theme === "dark" ? "☀" : "◐";
    };
    syncTheme();
    themeBtn.addEventListener("click", () => {
      const next = root.dataset.theme === "dark" ? "light" : "dark";
      root.dataset.theme = next;
      store.set("miniob-theme", next);
      syncTheme();
    });
  }

  // 2. Mobile drawer navigation
  const menuBtn = document.getElementById("menuBtn");
  const scrim = document.getElementById("navScrim");
  const closeNav = () => {
    document.body.classList.remove("nav-open");
    if (menuBtn) menuBtn.setAttribute("aria-expanded", "false");
  };
  if (menuBtn) {
    menuBtn.addEventListener("click", () => {
      const open = document.body.classList.toggle("nav-open");
      menuBtn.setAttribute("aria-expanded", String(open));
    });
  }
  if (scrim) scrim.addEventListener("click", closeNav);
  document.querySelectorAll(".sidebar a").forEach((link) =>
    link.addEventListener("click", closeNav)
  );

  // 3. Reading progress bar & Sticky TOC scroll-spy
  const progressEl = document.getElementById("readingProgress");
  const tocLinks = [...document.querySelectorAll(".page-toc .toc-item a")];
  const tocTargets = tocLinks
    .map((link) => {
      const hash = link.getAttribute("href") || "";
      if (!hash.startsWith("#") || hash.length < 2) return null;
      const target = document.getElementById(decodeURIComponent(hash.slice(1)));
      return target ? { link, target } : null;
    })
    .filter(Boolean);

  const updateScrollState = () => {
    if (progressEl) {
      const docH = document.documentElement.scrollHeight - window.innerHeight;
      const pct = docH > 40 ? Math.min(100, Math.max(0, (window.scrollY / docH) * 100)) : 0;
      progressEl.style.width = `${pct.toFixed(1)}%`;
    }
    if (tocTargets.length) {
      const threshold = 110;
      let activeItem = tocTargets[0];
      for (const item of tocTargets) {
        if (item.target.getBoundingClientRect().top <= threshold) {
          activeItem = item;
        } else {
          break;
        }
      }
      tocLinks.forEach((l) => l.classList.toggle("active", l === activeItem.link));
    }
  };
  window.addEventListener("scroll", updateScrollState, { passive: true });
  updateScrollState();

  // 4. Syntax highlighting & one-click code copy buttons
  document.querySelectorAll(".content pre").forEach((pre) => {
    const code = pre.querySelector("code");
    if (code && window.hljs) {
      try { window.hljs.highlightElement(code); } catch (e) { /* ignore */ }
    }
    if (pre.parentElement && !pre.parentElement.classList.contains("code-block-wrap")) {
      const wrap = document.createElement("div");
      wrap.className = "code-block-wrap";
      pre.parentNode.insertBefore(wrap, pre);
      wrap.appendChild(pre);
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "code-copy-btn";
      btn.textContent = "复制";
      btn.setAttribute("aria-label", "复制代码");
      btn.addEventListener("click", async () => {
        const text = (code || pre).innerText;
        try {
          await navigator.clipboard.writeText(text);
          btn.textContent = "✓ 已复制";
          setTimeout(() => { btn.textContent = "复制"; }, 1600);
        } catch (err) {
          btn.textContent = "复制失败";
          setTimeout(() => { btn.textContent = "复制"; }, 1600);
        }
      });
      wrap.appendChild(btn);
    }
  });

  // 5. Structure Tree <-> Markmap Toggle
  document.querySelectorAll("[data-toggle-markmap]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const panel = btn.closest(".map-panel");
      if (!panel) return;
      const treeView = panel.querySelector(".tree-view");
      const mmWrap = panel.querySelector(".markmap-wrap");
      if (!mmWrap) return;
      const willShowMm = mmWrap.hidden;
      mmWrap.hidden = !willShowMm;
      if (treeView) treeView.hidden = willShowMm;
      btn.setAttribute("aria-expanded", String(willShowMm));
      if (treeView) {
        btn.textContent = willShowMm ? "⊞ 切换章节卡片" : "⊞ 切换思维导图";
      } else {
        btn.textContent = willShowMm ? "⊟ 收起导图" : "⊞ 展开导图";
      }
      if (willShowMm && window.markmap && window.markmap.autoLoader) {
        try { window.markmap.autoLoader.renderAll(); } catch (e) { /* ignore */ }
      }
    });
  });

  // 6. Fullscreen Diagram / Image Zoom Modal
  const modal = document.getElementById("diagramModal");
  const modalTitle = document.getElementById("diagramModalTitle");
  const modalBody = document.getElementById("diagramModalBody");
  const modalClose = document.getElementById("diagramModalClose");

  const openModal = (title, contentNode) => {
    if (!modal || !modalBody) return;
    if (modalTitle) modalTitle.textContent = title || "图表检视";
    modalBody.innerHTML = "";
    modalBody.appendChild(contentNode);
    modal.hidden = false;
    document.body.style.overflow = "hidden";
  };
  const closeModal = () => {
    if (!modal || modal.hidden) return;
    modal.hidden = true;
    modalBody.innerHTML = "";
    document.body.style.overflow = "";
  };
  if (modalClose) modalClose.addEventListener("click", closeModal);
  if (modal) {
    modal.addEventListener("click", (e) => {
      if (e.target === modal || e.target === modalBody) closeModal();
    });
  }
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && modal && !modal.hidden) closeModal();
  });

  document.querySelectorAll("[data-zoom-diagram]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const container = btn.closest(".arch-figure, .flow-player");
      if (!container) return;
      const svg = container.querySelector("svg.toolkit-diagram");
      const heading = container.querySelector("h2, h3");
      if (!svg) return;
      const clone = svg.cloneNode(true);
      openModal(heading ? heading.textContent : "架构与流程图检视", clone);
    });
  });

  document.querySelectorAll(".content img").forEach((img) => {
    img.addEventListener("click", () => {
      const clone = img.cloneNode(true);
      openModal(img.alt || "图片检视", clone);
    });
  });

  // 7. Home search filter
  const search = document.getElementById("docSearch");
  if (search) {
    const cards = [...document.querySelectorAll(".doc-card")];
    const groups = [...document.querySelectorAll(".group-block")];
    const empty = document.getElementById("emptyState");
    const filter = () => {
      const query = search.value.trim().toLowerCase();
      let visible = 0;
      cards.forEach((card) => {
        const haystack = (card.dataset.search || card.textContent || "").toLowerCase();
        const hit = !query || haystack.includes(query);
        card.hidden = !hit;
        if (hit) visible += 1;
      });
      groups.forEach((group) => {
        group.hidden = !group.querySelector(".doc-card:not([hidden])");
      });
      if (empty) empty.hidden = visible > 0;
    };
    search.addEventListener("input", filter);
    document.addEventListener("keydown", (event) => {
      if (
        event.key === "/" &&
        document.activeElement !== search &&
        !/^(INPUT|TEXTAREA)$/.test(document.activeElement.tagName)
      ) {
        event.preventDefault();
        search.focus();
      }
    });
  }

  // 8. Interactive Deterministic SVG Flow Players
  const dataEl = document.getElementById("flow-data");
  if (!dataEl) return;
  let flows = [];
  try {
    flows = JSON.parse(dataEl.textContent);
  } catch (e) {
    return;
  }
  const byId = new Map(flows.map((flow) => [flow.id, flow]));

  document.querySelectorAll(".flow-player").forEach((player) => {
    const flow = byId.get(player.dataset.flow);
    if (!flow) return;
    const nodeLabelMap = new Map(flow.nodes.map((n) => [n.id, n.label]));
    const svg = player.querySelector("svg.toolkit-diagram");
    const svgNodes = svg ? [...svg.querySelectorAll(".flow-node")] : [];
    const svgEdges = svg ? [...svg.querySelectorAll(".flow-edge")] : [];
    const progressBar = player.querySelector(".flow-progress-bar");
    const count = player.querySelector(".flow-count");
    const descBadge = player.querySelector(".flow-desc-badge");
    const descTitle = player.querySelector(".flow-desc-title");
    const descTags = player.querySelector(".flow-desc-tags");
    const descText = player.querySelector(".flow-desc-text");
    const playBtn = player.querySelector('[data-act="play"]');
    const prevBtn = player.querySelector('[data-act="prev"]');
    const nextBtn = player.querySelector('[data-act="next"]');
    const chips = [...player.querySelectorAll(".step-chip")];
    const total = flow.steps.length;
    let step = 0;
    let timer = null;

    const stop = () => {
      if (timer) {
        clearInterval(timer);
        timer = null;
      }
      if (playBtn) playBtn.textContent = "▶ 自动播放";
    };

    const paint = () => {
      const currentStep = flow.steps[step];
      const activeSet = new Set(currentStep.highlight || []);
      const visitedSet = new Set();
      for (let i = 0; i < step; i++) {
        (flow.steps[i].highlight || []).forEach((id) => {
          if (!activeSet.has(id)) visitedSet.add(id);
        });
      }

      if (count) count.textContent = `步骤 ${step + 1} / ${total}`;
      if (descBadge) descBadge.textContent = `STEP ${step + 1} / ${total}`;
      if (descTitle) descTitle.textContent = currentStep.title;
      if (descText) descText.textContent = currentStep.desc;
      if (descTags) {
        descTags.innerHTML = [...activeSet]
          .map((id) => `<span class="flow-tag">${escapeHtml(nodeLabelMap.get(id) || id)}</span>`)
          .join("");
      }
      if (progressBar) {
        const pct = ((step + 1) / Math.max(1, total)) * 100;
        progressBar.style.width = `${pct.toFixed(1)}%`;
      }

      chips.forEach((chip, index) => {
        chip.classList.toggle("active", index === step);
        chip.classList.toggle("visited", index < step);
        chip.setAttribute("aria-selected", String(index === step));
      });

      if (svg) {
        svg.classList.add("is-stepping");
        svgNodes.forEach((nodeEl) => {
          const nid = nodeEl.getAttribute("data-node-id");
          const isActive = activeSet.has(nid);
          const isVisited = !isActive && visitedSet.has(nid);
          nodeEl.classList.toggle("is-active", isActive);
          nodeEl.classList.toggle("is-visited", isVisited);
          nodeEl.classList.toggle("is-dim", !isActive && !isVisited);
        });

        svgEdges.forEach((edgeEl) => {
          const from = edgeEl.getAttribute("data-from");
          const to = edgeEl.getAttribute("data-to");
          const isActive =
            (activeSet.has(from) && activeSet.has(to)) ||
            (activeSet.has(to) && visitedSet.has(from)) ||
            (activeSet.has(from) && !activeSet.has(to) && step < total - 1 && (flow.steps[step + 1].highlight || []).includes(to));
          const isVisited =
            !isActive &&
            (visitedSet.has(from) || activeSet.has(from)) &&
            (visitedSet.has(to) || activeSet.has(to));
          edgeEl.classList.toggle("is-active", isActive);
          edgeEl.classList.toggle("is-visited", isVisited);
          edgeEl.classList.toggle("is-dim", !isActive && !isVisited);
        });
      }
    };

    const start = () => {
      stop();
      if (step >= total - 1) step = 0;
      paint();
      if (playBtn) playBtn.textContent = "⏸ 暂停播放";
      timer = setInterval(() => {
        if (step >= total - 1) {
          stop();
          return;
        }
        step += 1;
        paint();
      }, 2500);
    };

    if (playBtn) playBtn.addEventListener("click", () => (timer ? stop() : start()));
    if (prevBtn) {
      prevBtn.addEventListener("click", () => {
        stop();
        step = (step - 1 + total) % total;
        paint();
      });
    }
    if (nextBtn) {
      nextBtn.addEventListener("click", () => {
        stop();
        step = (step + 1) % total;
        paint();
      });
    }
    chips.forEach((chip) =>
      chip.addEventListener("click", () => {
        stop();
        step = Number(chip.dataset.step);
        paint();
      })
    );

    // Click or Enter on any SVG node to jump to its step
    svgNodes.forEach((nodeEl) => {
      const activateNode = () => {
        const rawSteps = nodeEl.getAttribute("data-steps");
        if (!rawSteps) return;
        const stepIndices = rawSteps.split(",").map(Number).filter((n) => !Number.isNaN(n));
        if (!stepIndices.length) return;
        stop();
        const nextIdx = stepIndices.find((s) => s > step) ?? stepIndices[0];
        step = nextIdx;
        paint();
      };
      nodeEl.addEventListener("click", activateNode);
      nodeEl.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          activateNode();
        }
      });
    });

    // Keyboard navigation on the flow player
    player.addEventListener("keydown", (e) => {
      if (e.target && e.target.closest(".flow-node")) return;
      if (e.key === "ArrowLeft") {
        e.preventDefault();
        stop();
        step = (step - 1 + total) % total;
        paint();
      } else if (e.key === "ArrowRight") {
        e.preventDefault();
        stop();
        step = (step + 1) % total;
        paint();
      } else if (e.key === " ") {
        e.preventDefault();
        timer ? stop() : start();
      }
    });

    paint();
  });

  function escapeHtml(value) {
    return String(value).replace(/[&<>"']/g, (char) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    }[char]));
  }
})();
