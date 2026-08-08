(() => {
  let handleEarlyKeydown = () => {};
  window.addEventListener("keydown", (event) => handleEarlyKeydown(event), true);

  const setup = () => {
    const toggle = document.querySelector("#__drawer");
    const drawer = document.querySelector(".md-sidebar--primary");
    const navigation = drawer?.querySelector(".md-nav--primary");
    const openControl = document.querySelector(".md-header__button[for='__drawer']");
    const backdrop = document.querySelector(".md-overlay[for='__drawer']");

    if (!(toggle instanceof HTMLInputElement) || !(drawer instanceof HTMLElement)) return;
    if (!navigation || !openControl || !backdrop) return;

    let opener = null;
    let inerted = [];
    let modalOpen = false;

    drawer.id = "__navigation";
    const closeControl = document.createElement("button");
    closeControl.className = "dw-navigation__close md-icon";
    closeControl.type = "button";
    closeControl.setAttribute("aria-label", "Close navigation");
    closeControl.setAttribute("aria-controls", "__navigation");
    closeControl.innerHTML = [
      '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" aria-hidden="true">',
      '<path d="M20 11v2H8l5.5 5.5-1.42 1.42L4.16 12l7.92-7.92L13.5 5.5 8 11z"/>',
      "</svg>",
    ].join("");
    navigation.prepend(closeControl);

    const makeButton = (control, label) => {
      control.setAttribute("role", "button");
      control.setAttribute("aria-label", label);
      control.setAttribute("aria-controls", "__navigation");
      control.tabIndex = 0;
    };

    const dispatchToggleChange = () => {
      toggle.dispatchEvent(new Event("change", { bubbles: true }));
    };

    const setOpen = (open) => {
      if (toggle.checked === open) return;
      toggle.checked = open;
      dispatchToggleChange();
    };

    const rememberOpener = () => {
      if (!toggle.checked) opener = openControl;
    };

    const handleButtonKey = (event, action) => {
      if (event.key !== "Enter" && event.key !== " ") return;
      event.preventDefault();
      event.stopImmediatePropagation();
      action();
    };

    const isolateDrawer = () => {
      if (inerted.length) return;
      let current = drawer;
      while (current.parentElement && current !== document.body) {
        const parent = current.parentElement;
        for (const sibling of parent.children) {
          if (sibling === current || sibling === toggle || sibling === backdrop) continue;
          if (["SCRIPT", "STYLE"].includes(sibling.tagName) || sibling.inert) continue;
          sibling.inert = true;
          inerted.push(sibling);
        }
        current = parent;
      }
    };

    const releaseDrawer = () => {
      for (const element of inerted) element.inert = false;
      inerted = [];
    };

    const visibleTabStops = () => [
      ...drawer.querySelectorAll("a[href], button, input, select, textarea, summary, [tabindex]"),
    ].filter((element) => {
      if (!(element instanceof HTMLElement) || element.closest("[inert]") || element.tabIndex < 0) return false;
      if (element.closest("details:not([open])") && !element.closest("summary")) return false;
      const style = getComputedStyle(element);
      const bounds = element.getBoundingClientRect();
      return style.display !== "none" && style.visibility !== "hidden" && bounds.width > 0 && bounds.height > 0;
    });

    const containTabFocus = (event) => {
      if (!modalOpen || event.key !== "Tab") return;
      const stops = visibleTabStops();
      event.preventDefault();
      event.stopImmediatePropagation();
      if (!stops.length) {
        closeControl.focus({ preventScroll: true });
        return;
      }
      const activeIndex = stops.indexOf(document.activeElement);
      const nextIndex = event.shiftKey
        ? (activeIndex <= 0 ? stops.length - 1 : activeIndex - 1)
        : (activeIndex < 0 || activeIndex === stops.length - 1 ? 0 : activeIndex + 1);
      stops[nextIndex].focus({ preventScroll: true });
    };

    const restoreOpenerFocus = () => {
      const target = opener;
      opener = null;
      if (target instanceof HTMLElement && target.isConnected) target.focus({ preventScroll: true });
    };

    const applyState = () => {
      openControl.setAttribute("aria-expanded", String(toggle.checked));
      if (toggle.checked) {
        modalOpen = true;
        drawer.setAttribute("role", "dialog");
        drawer.setAttribute("aria-modal", "true");
        drawer.setAttribute("aria-label", "Navigation");
        isolateDrawer();
        closeControl.focus({ preventScroll: true });
        return;
      }

      const wasOpen = modalOpen;
      modalOpen = false;
      drawer.removeAttribute("role");
      drawer.removeAttribute("aria-modal");
      drawer.removeAttribute("aria-label");
      releaseDrawer();
      if (wasOpen) restoreOpenerFocus();
    };

    makeButton(openControl, "Open navigation");
    openControl.setAttribute("aria-haspopup", "dialog");
    backdrop.setAttribute("aria-label", "Close navigation");
    backdrop.setAttribute("aria-controls", "__navigation");

    openControl.addEventListener("pointerdown", rememberOpener);
    openControl.addEventListener("keydown", (event) => {
      handleButtonKey(event, () => {
        rememberOpener();
        setOpen(true);
      });
    });
    closeControl.addEventListener("click", () => setOpen(false));
    toggle.addEventListener("change", applyState);

    handleEarlyKeydown = (event) => {
      containTabFocus(event);
      if (event.defaultPrevented) return;
      if (modalOpen && event.key === "Escape") {
        event.preventDefault();
        event.stopImmediatePropagation();
        setOpen(false);
      }
    };
    document.addEventListener("focusin", (event) => {
      if (modalOpen && !drawer.contains(event.target)) closeControl.focus({ preventScroll: true });
    });

    applyState();
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", setup, { once: true });
  } else {
    setup();
  }
})();
