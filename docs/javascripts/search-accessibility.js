(() => {
  let handleEarlyKeydown = () => {};
  window.addEventListener("keydown", (event) => handleEarlyKeydown(event), true);

  const setup = () => {
    const toggle = document.querySelector("#__search");
    const dialog = document.querySelector(".md-search[role='dialog']");
    const input = dialog?.querySelector(".md-search__input");
    const openControl = document.querySelector(".md-header__button[for='__search']");
    const closeControl = dialog?.querySelector(".md-search__icon[for='__search']");

    if (!(toggle instanceof HTMLInputElement) || !dialog || !(input instanceof HTMLInputElement)) return;
    if (!openControl || !closeControl) return;

    let opener = null;
    let inerted = [];
    let modalOpen = false;
    let restoringFocus = false;
    let suppressRestoredInputOpen = false;

    const makeButton = (control, label) => {
      control.setAttribute("role", "button");
      control.setAttribute("aria-label", label);
      control.setAttribute("aria-controls", "__search");
    };

    const dispatchToggleChange = () => {
      toggle.dispatchEvent(new Event("change", { bubbles: true }));
    };

    const setOpen = (open) => {
      if (toggle.checked === open) return;
      toggle.checked = open;
      dispatchToggleChange();
    };

    const rememberOpener = (control) => {
      if (!toggle.checked && !restoringFocus) opener = control;
    };

    const openSearch = (control) => {
      rememberOpener(control);
      setOpen(true);
      input.focus({ preventScroll: true });
    };

    const closeSearch = () => setOpen(false);

    const handleButtonKey = (event, action) => {
      if (event.key !== "Enter" && event.key !== " ") return;
      event.preventDefault();
      event.stopImmediatePropagation();
      action();
    };

    const isolateDialog = () => {
      if (inerted.length) return;
      let current = dialog;
      while (current.parentElement && current !== document.body) {
        const parent = current.parentElement;
        for (const sibling of parent.children) {
          if (sibling === current || sibling === toggle || ["SCRIPT", "STYLE"].includes(sibling.tagName)) continue;
          if (!sibling.inert) {
            sibling.inert = true;
            inerted.push(sibling);
          }
        }
        current = parent;
      }
    };

    const releaseDialog = () => {
      for (const element of inerted) element.inert = false;
      inerted = [];
    };

    const visibleTabStops = () => {
      const stops = [
        ...dialog.querySelectorAll("a[href], button, input, select, textarea, summary, [tabindex]"),
      ].filter((element) => {
        if (!(element instanceof HTMLElement) || element.closest("[inert]")) return false;
        const isResultControl = element.matches(".md-search-result a[href], .md-search-result summary");
        if (element.tabIndex < 0 && !isResultControl) return false;
        if (element.closest("details:not([open])") && !element.closest("summary")) return false;
        const style = getComputedStyle(element);
        const bounds = element.getBoundingClientRect();
        return style.display !== "none" && style.visibility !== "hidden" && bounds.width > 0 && bounds.height > 0;
      });
      const scrollControl = dialog.querySelector(".md-search__scrollwrap");
      const ordered = stops.filter((element) => element !== closeControl && element !== scrollControl);
      if (scrollControl instanceof HTMLElement && stops.includes(scrollControl)) ordered.push(scrollControl);
      if (stops.includes(closeControl)) ordered.push(closeControl);
      return ordered;
    };

    const containTabFocus = (event) => {
      if (!modalOpen || event.key !== "Tab") return;
      const stops = visibleTabStops();
      if (!stops.length) {
        event.preventDefault();
        event.stopImmediatePropagation();
        input.focus({ preventScroll: true });
        return;
      }

      event.preventDefault();
      event.stopImmediatePropagation();
      const activeIndex = stops.indexOf(document.activeElement);
      const nextIndex = event.shiftKey
        ? (activeIndex <= 0 ? stops.length - 1 : activeIndex - 1)
        : (activeIndex < 0 || activeIndex === stops.length - 1 ? 0 : activeIndex + 1);
      stops[nextIndex].focus({ preventScroll: true });
    };

    const restoreOpenerFocus = () => {
      const target = opener;
      opener = null;
      if (!(target instanceof HTMLElement) || !target.isConnected) return;

      restoringFocus = true;
      suppressRestoredInputOpen = target === input;
      target.focus({ preventScroll: true });

      // Material opens its desktop input on focus. Keep the restored focus while
      // preserving the close action that initiated this restoration.
      if (toggle.checked) {
        toggle.checked = false;
        dispatchToggleChange();
      }
      queueMicrotask(() => {
        restoringFocus = false;
      });
    };

    const applyState = () => {
      if (toggle.checked) {
        if (restoringFocus || (suppressRestoredInputOpen && document.activeElement === input)) {
          toggle.checked = false;
          dispatchToggleChange();
          return;
        }
        modalOpen = true;
        dialog.setAttribute("aria-modal", "true");
        makeButton(closeControl, "Close search");
        closeControl.tabIndex = 0;
        isolateDialog();
        if (!dialog.contains(document.activeElement)) input.focus({ preventScroll: true });
        return;
      }

      modalOpen = false;
      dialog.removeAttribute("aria-modal");
      closeControl.removeAttribute("role");
      closeControl.removeAttribute("aria-label");
      closeControl.removeAttribute("aria-controls");
      closeControl.tabIndex = -1;
      releaseDialog();
      if (!restoringFocus) restoreOpenerFocus();
    };

    makeButton(openControl, "Open search");
    openControl.tabIndex = 0;
    closeControl.tabIndex = -1;

    openControl.addEventListener("pointerdown", () => rememberOpener(openControl));
    openControl.addEventListener("keydown", (event) => handleButtonKey(event, () => openSearch(openControl)));
    input.addEventListener("pointerdown", () => {
      suppressRestoredInputOpen = false;
      rememberOpener(input);
    });
    input.addEventListener("click", () => {
      if (!toggle.checked) openSearch(input);
    });
    input.addEventListener("keydown", (event) => {
      if (!toggle.checked && !["Escape", "Shift", "Tab"].includes(event.key)) {
        suppressRestoredInputOpen = false;
        openSearch(input);
      }
    });
    input.addEventListener("blur", () => {
      if (!modalOpen) suppressRestoredInputOpen = false;
    });
    input.addEventListener("focus", () => {
      if (opener === null && !restoringFocus) opener = input;
    });
    closeControl.addEventListener("keydown", (event) => handleButtonKey(event, closeSearch));
    toggle.addEventListener("change", applyState);
    handleEarlyKeydown = (event) => {
      if (modalOpen && event.key === "Shift" && dialog.contains(document.activeElement)) {
        event.stopImmediatePropagation();
        return;
      }
      if (modalOpen && document.activeElement === closeControl && ["Enter", " "].includes(event.key)) {
        event.preventDefault();
        event.stopImmediatePropagation();
        closeSearch();
        return;
      }
      containTabFocus(event);
      if (event.defaultPrevented) return;
      if (modalOpen && event.key === "Escape") {
        event.preventDefault();
        event.stopImmediatePropagation();
        closeSearch();
      }
    };
    document.addEventListener("focusin", (event) => {
      if (modalOpen && !dialog.contains(event.target)) {
        setOpen(true);
        input.focus({ preventScroll: true });
      }
    });

    applyState();
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", setup, { once: true });
  } else {
    setup();
  }
})();
