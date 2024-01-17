Object.defineProperty(Object.prototype, 'pad', {
  value: function (width, z) {
    z = z || '0';
    let n = this.toString();
    return n.length >= width ? n : new Array(width - n.length + 1).join(z) + n;
  }
});