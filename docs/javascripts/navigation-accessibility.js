(() => {
  let handleEarlyKeydown = () => {};
  window.addEventListener("keydown", (event) => handleEarlyKeydown(event), true);

  const setup = () => {
    const toggle = document.querySelector("#__drawer");
    const drawer = document.querySelector(".md-sidebar--primary");
    const navigation = drawer?.querySelector(".md-nav--primary");
    const openControl = document.querySelector(".md-header__button[for='__drawer']");
    const backdrop = document.querySelector(".md-overlay[for='__drawer']");
    const desktopLayout = window.matchMedia("screen and (min-width: 60em)");

    if (!(toggle instanceof HTMLInputElement) || !(drawer instanceof HTMLElement)) return;
    if (!navigation || !openControl || !backdrop) return;

    let opener = null;
    let inerted = [];
    let panelInerted = [];
    let modalOpen = false;

    drawer.id = "__navigation";
    const closeControl = document.createElement("button");
    closeControl.className = "dw-navigation__close md-icon";
    closeControl.type = "button";
    closeControl.dataset.navigationAction = "close";
    closeControl.setAttribute("aria-label", "Close navigation");
    closeControl.setAttribute("aria-controls", "__navigation");
    closeControl.innerHTML = [
      '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" aria-hidden="true">',
      '<path d="M19 6.41 17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z"/>',
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

    const activeNavigationPanel = () => {
      let activePanel = navigation;
      for (const panelToggle of drawer.querySelectorAll("input.md-nav__toggle:checked")) {
        const panel = panelToggle.parentElement?.querySelector(":scope > nav.md-nav");
        if (panel instanceof HTMLElement && activePanel.contains(panel)) activePanel = panel;
      }
      return activePanel;
    };

    const releaseInactivePanelControls = () => {
      for (const element of panelInerted) element.inert = false;
      panelInerted = [];
    };

    const isolateActivePanel = () => {
      releaseInactivePanelControls();
      if (!modalOpen) return;

      const activePanel = activeNavigationPanel();
      const inactivePanels = [...drawer.querySelectorAll("input.md-nav__toggle:not(:checked)")]
        .map((panelToggle) => panelToggle.parentElement?.querySelector(":scope > nav.md-nav"))
        .filter((panel) => panel instanceof HTMLElement);
      for (const element of drawer.querySelectorAll("a[href], button, summary, [tabindex]")) {
        const inActivePanel = activePanel.contains(element)
          && !inactivePanels.some((panel) => panel.contains(element));
        if (element === closeControl || inActivePanel || element.closest("[inert]")) continue;
        element.inert = true;
        panelInerted.push(element);
      }
    };

    const revealActiveDestination = () => {
      const activeDestination = activeNavigationPanel().querySelector(".md-nav__link--active");
      if (activeDestination instanceof HTMLElement) {
        activeDestination.scrollIntoView({ block: "center", inline: "nearest" });
      }
    };

    const isRendered = (element) => {
      if (!(element instanceof HTMLElement) || element.closest("[inert]") || element.tabIndex < 0) return false;
      const style = getComputedStyle(element);
      const bounds = element.getBoundingClientRect();
      return style.display !== "none" && style.visibility !== "hidden"
        && bounds.width > 0 && bounds.height > 0;
    };

    const isReachablyRendered = (element) => {
      if (!isRendered(element)) return false;
      const bounds = element.getBoundingClientRect();
      return bounds.right > 0 && bounds.bottom > 0 && bounds.left < innerWidth && bounds.top < innerHeight;
    };

    const visibleTabStops = () => [
      ...drawer.querySelectorAll("a[href], button, input, select, textarea, summary, [tabindex]"),
    ].filter((element) => {
      if (!isRendered(element)) return false;
      return !element.closest("details:not([open])") || Boolean(element.closest("summary"));
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
      if (target instanceof HTMLElement && target.isConnected && isReachablyRendered(target)) {
        target.focus({ preventScroll: true });
        return;
      }
      if (desktopLayout.matches) visibleTabStops()[0]?.focus({ preventScroll: true });
    };

    const applyState = () => {
      openControl.setAttribute("aria-expanded", String(toggle.checked));
      if (toggle.checked) {
        modalOpen = true;
        drawer.setAttribute("role", "dialog");
        drawer.setAttribute("aria-modal", "true");
        drawer.setAttribute("aria-label", "Navigation");
        isolateDrawer();
        isolateActivePanel();
        revealActiveDestination();
        closeControl.focus({ preventScroll: true });
        return;
      }

      const wasOpen = modalOpen;
      modalOpen = false;
      drawer.removeAttribute("role");
      drawer.removeAttribute("aria-modal");
      drawer.removeAttribute("aria-label");
      releaseInactivePanelControls();
      releaseDrawer();
      if (wasOpen) restoreOpenerFocus();
    };

    const reconcileLayout = () => {
      if (desktopLayout.matches && toggle.checked) {
        setOpen(false);
        return;
      }

      applyState();
      if (
        !desktopLayout.matches
        && document.activeElement instanceof HTMLElement
        && !isReachablyRendered(document.activeElement)
      ) {
        openControl.focus({ preventScroll: true });
      }
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
    toggle.addEventListener("change", reconcileLayout);
    drawer.addEventListener("change", isolateActivePanel);
    desktopLayout.addEventListener("change", reconcileLayout);

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

    reconcileLayout();
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", setup, { once: true });
  } else {
    setup();
  }
})();
