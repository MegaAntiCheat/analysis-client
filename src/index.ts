require('dotenv').config();

async function main(): Promise<void> {

    const interval = 60 * 1000;

    while (true) {
        const start = Date.now();

        console.log("Starting iteration...");

        console.log("Checking for jobs...");

        




        const end = Date.now();
        const duration = end - start;
        if (interval - duration > 0) {
            await new Promise(resolve => setTimeout(resolve, interval - duration));
        }
    }
}

main();