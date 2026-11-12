// Ausgelagert, weil die Content Security Policy kein Inline-JavaScript erlaubt.
document.addEventListener("DOMContentLoaded", () => {
  // Nicht ladbare Kartenbilder ausblenden statt ein kaputtes Bild zu zeigen
  document.querySelectorAll("img").forEach((img) => {
    img.addEventListener("error", () => img.classList.add("missing"));
  });

  // "zurück zur Suche" behält Suchbegriff und Seite, wenn man von dort kam
  const back = document.querySelector("[data-back]");
  if (back && document.referrer.startsWith(location.origin)) {
    back.addEventListener("click", (e) => {
      e.preventDefault();
      history.back();
    });
  }
});
