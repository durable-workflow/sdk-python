import { watchPublicStatus } from "./status.mjs";

/* Native disclosure semantics retain links and keyboard navigation without JS. */
(() => {
  const close = (switcher, restoreFocus) => {
    if (!switcher.open) return;
    switcher.open = false;
    if (restoreFocus) switcher.querySelector("summary").focus();
  };
  document.querySelectorAll("[data-sdk-switcher]").forEach((switcher) => {
    switcher.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && switcher.open) {
        event.preventDefault();
        close(switcher, true);
      }
    });
    document.addEventListener("click", (event) => {
      if (!switcher.contains(event.target)) close(switcher, false);
    });
  });

  document.querySelectorAll('.md-header__option label[for^="__palette_"]').forEach((control) => {
    control.setAttribute("role", "button");
    control.setAttribute("aria-label", control.title);
    control.tabIndex = 0;
    control.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        // Material also handles Enter globally; one activation changes the theme once.
        event.stopPropagation();
        control.click();
        requestAnimationFrame(() => {
          document.querySelector('.md-header__option input:checked')?.nextElementSibling?.focus();
        });
      }
    });
  });

  const pill = document.querySelector("[data-public-status]");
  if (!pill) return;
  const labels = {
    operational: "All systems operational",
    degraded: "Degraded performance",
    incident: "Service incident",
    maintenance: "Maintenance",
    stale: "Status stale",
    unverified: "Status unverified",
    unavailable: "Status unavailable",
    unknown: "Status unknown",
  };
  const renderStatus = (state) => {
    const safeState = Object.hasOwn(labels, state) ? state : "unknown";
    if (pill.dataset.publicStatus === safeState) return;
    pill.dataset.publicStatus = safeState;
    pill.querySelector("[data-status-label]").textContent = labels[safeState];
  };
  let stopStatus;
  const startStatus = () => {
    stopStatus?.();
    renderStatus("unknown");
    stopStatus = watchPublicStatus(renderStatus);
  };
  startStatus();
  window.addEventListener("pagehide", () => {
    stopStatus?.();
    stopStatus = undefined;
    renderStatus("unknown");
  });
  window.addEventListener("pageshow", (event) => {
    if (event.persisted) startStatus();
  });
})();
