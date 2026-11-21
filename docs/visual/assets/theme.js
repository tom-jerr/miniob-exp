try {
  document.documentElement.classList.add("js");
  var saved = localStorage.getItem("miniob-theme");
  if (saved) {
    document.documentElement.dataset.theme = saved;
  } else if (window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches) {
    document.documentElement.dataset.theme = "dark";
  }
} catch (error) {
  // privacy modes can block localStorage; defaults are fine
}
