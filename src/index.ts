import axios from 'axios';
import { ChildProcess, execFile, execFileSync, spawn, SpawnOptions } from 'child_process';
import { existsSync, mkdir, mkdirSync, rmdirSync, rmSync } from 'fs';
import { promisify } from 'util';
import fs from 'fs';
import path from 'path';
import { Client } from 'minio';

require('dotenv').config();

const longInterval = 60 * 1000;
const shortInterval = 1 * 1000;
const PROCESS_LIMIT: number = Number(process.env.PROCESS_LIMIT) || 1;
const QUERY_LIMIT: number = Number(process.env.QUERY_LIMIT) || 1;
const JOBS_URL: string = process.env.JOBS_URL || "";
const KEY: string = process.env.KEY || "";
const ANALYSIS_EXECUTABLE: string = process.env.ANALYSIS_EXECUTABLE || "";
const MINIO_HOSTNAME: string = process.env.MINIO_HOSTNAME || "";
const MINIO_ACCESS_KEY: string = process.env.MINIO_ACCESS_KEY || "";
const MINIO_SECRET_KEY: string = process.env.MINIO_SECRET_KEY || "";

async function main(): Promise<void> {

    const tempPath = path.join(process.cwd(), 'temp');
    const demoPath = path.join(tempPath, 'demo');
    const jsonPath = path.join(tempPath, 'json');

    if (existsSync(tempPath)) rmdirSync(tempPath, { recursive: true });
    mkdirSync(tempPath);
    mkdirSync(demoPath);
    mkdirSync(jsonPath);

    let queue: Set<string> = new Set<string>();
    let running_jobs: Map<string, Promise<string>> = new Map<string, Promise<string>>();

    do {
        const start = Date.now();

        //console.log("Starting iteration...");

        // Check for sessions to add to queue.
        if (queue.size >= PROCESS_LIMIT) {
            //console.log(`${queue.size} sessions in local queue. We have enough for now.`);
        } else {
            console.log(`${queue.size} sessions in local queue. Querying for more...`);
            const response = await axios.get(`${JOBS_URL}?api_key=${KEY}&limit=${QUERY_LIMIT}`);

            if (response.status !== 200) {
                console.log(`Failed to get jobs for queue: ${response.status} - ${response.statusText}\n${response.data}`);
                await delay(start, shortInterval);
                continue;
            }

            let old_queue_size = queue.size;
            response.data.forEach((job: any) => {
                queue.add(job);
            });

            console.log(`Added ${queue.size - old_queue_size} sessions to queue (${queue.size} total).`);
        }

        let session_ids = Array.from(queue);
        
        // Run the jobs.
        while (running_jobs.size < PROCESS_LIMIT) {
            const session_id = session_ids.shift();
            if (session_id === undefined) {
                break;
            } else if (running_jobs.has(session_id)) {
                continue;
            }
            console.log("Starting job: " + session_id);
            const promise = do_job(session_id);
            running_jobs.set(session_id, promise);
            queue.delete(session_id);
        }

        // Check the jobs
        const running_jobs_copy = new Map(running_jobs);
        for (const [session_id, promise] of running_jobs_copy) {
            await Promise.race([promise, Promise.resolve("not_done")]).then((result) => {
                if (result === "not_done") {
                    return;
                } else if(result === "") {
                    console.log("Job completed successfully: " + session_id);
                } else {
                    console.log("Job failed: " + session_id + "\n" + result);
                    handle_failed_job(session_id, result);
                }
                running_jobs.delete(session_id);
            })
        }

        await delay(start, (running_jobs.size === 0 && queue.size === 0) ? longInterval : shortInterval);   
    } while (true);
}

async function download_demo_data(session_id: string): Promise<string> {
    // Download demo

    try {
        const inPath = path.join(process.cwd(), 'temp', 'demo', `${session_id}.dem`);
        const minioClient = new Client({
            endPoint: MINIO_HOSTNAME,
            port: 9000,
            useSSL: false,
            accessKey: MINIO_ACCESS_KEY,
            secretKey: MINIO_SECRET_KEY
        });
        const data = await minioClient.getObject('demoblobs', `${session_id}.dem`);

        const stream = fs.createWriteStream(inPath);
        data.pipe(stream);
        await new Promise((resolve, reject) => {
            stream.on('finish', resolve);
            stream.on('error', reject);
        });
        
    } catch (error) {
        if(error instanceof Error) {
            return error.message;   
        } else {
            return "Unknown error";
        }
    }
    return "";
}

async function do_job(session_id: string): Promise<string> {

    const inPath = path.join(process.cwd(), 'temp', 'demo', `${session_id}.dem`);
    const outPath = path.join(process.cwd(), 'temp', 'json', `${session_id}.json`);

    try {

        const result =await download_demo_data(session_id);

        if(result !== "") {
            throw new Error(result);
        }
        console.log("Downloaded demo " + session_id);

        // Run analysis
        const child_process = execFile(
            `"${ANALYSIS_EXECUTABLE}"`,
            ["-q", "-i", inPath],
            {
                shell: true,
                timeout: 60000,
                cwd: process.cwd()
            } as SpawnOptions
        );
        const output = await new Promise<string>((resolve, reject) => {
            let stdout = '';
            child_process.stdout?.setEncoding('utf8');
            child_process.stdout?.on('data', (data) => stdout += data);
            child_process.on('error', reject);
            child_process.on('close', () => resolve(stdout));
        });

        // Write json
        fs.writeFileSync(outPath, output);
        
    } catch (error) {
        
        if (error instanceof Error) {
            console.log("Job failed: " + session_id + "\n" + error.message);
            return error.message;   
        } else {
            console.log("Job failed for unknown reason: " + session_id);
            return "Unknown error";
        }
    } finally {
        if(existsSync(inPath)) rmSync(inPath);
        // if(existsSync(outPath)) rmSync(outPath);
    }
    console.log("Job completed successfully: " + session_id);
    return "";
}

async function handle_failed_job(session_id: string, error: string) {
    if (error === "") {
        return;
    }
}

async function delay(start: number, length: number) {
    const end = Date.now();
    const duration = end - start;
    if (length - duration > 0) {
        await new Promise(resolve => setTimeout(resolve, length - duration));
    }
}

main();