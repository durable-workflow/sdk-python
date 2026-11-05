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
})();
