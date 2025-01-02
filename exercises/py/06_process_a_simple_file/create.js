// Generate 10000 random numbers between 1 and 1000000
let numbers = Array.from({length: 10000}, () => Math.floor(Math.random() * 1000000) + 1);
console.log(numbers.join('\n'));
