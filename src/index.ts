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
const INGEST_URL: string = process.env.INGEST_URL || "";
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
    let completed_jobs: Map<string, number> = new Map<string, number>();

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
                if (queue.has(job) ||
                    running_jobs.has(job) ||
                    completed_jobs.has(job)) return;
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
                completed_jobs.set(session_id, Date.now());
                running_jobs.delete(session_id);
            })
        }

        if (running_jobs.size === 0 && queue.size === 0) {
            console.log("Queue completed");
            process.exit(0);
            await delay(start, longInterval);
        } 

        const clear_time = Date.now() - (2 * longInterval);
        const completed_jobs_copy = new Map(completed_jobs);
        for (const [session_id, timestamp] of completed_jobs_copy) {
            if (timestamp < clear_time) {
                completed_jobs.delete(session_id);
            }
        }

        await delay(Date.now(), shortInterval);
    
    } while (true);
}

function get_in_path(session_id: string): string {
    return path.join(process.cwd(), 'temp', 'demo', `${session_id}.dem`);
}

function get_out_path(session_id: string): string {
    return path.join(process.cwd(), 'temp', 'json', `${session_id}.json`);
}

async function do_job(session_id: string): Promise<string> {
    try {

        await download_demo_data(session_id);
        // console.log("Downloaded demo " + session_id);

        await analyse_demo(session_id);
        // console.log("Analysed demo " + session_id);

        await upload_analysis(session_id);
        // console.log("Uploaded analysis " + session_id);

        await mark_ingested(session_id);
        
    } catch (error) {
        
        if (error instanceof Error) {
            console.log("Job failed: " + session_id + "\n" + error.message);
            return error.message;   
        } else {
            console.log("Job failed for unknown reason: " + session_id);
            return "Unknown error";
        }
    } finally {
        const inPath = get_in_path(session_id);
        const outPath = get_out_path(session_id);
        if(existsSync(inPath)) rmSync(inPath);
        if(existsSync(outPath)) rmSync(outPath);
    }
    return "";
}

async function download_demo_data(session_id: string): Promise<void> {
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
}

async function analyse_demo(session_id: string): Promise<void> {
    const child_process = execFile(
        `"${ANALYSIS_EXECUTABLE}"`,
        ["-q", "-i", get_in_path(session_id)],
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
    fs.writeFileSync(get_out_path(session_id), output);
}

async function upload_analysis(session_id: string): Promise<void> {
    const minioClient = new Client({
        endPoint: MINIO_HOSTNAME,
        port: 9000,
        useSSL: false,
        accessKey: MINIO_ACCESS_KEY,
        secretKey: MINIO_SECRET_KEY
    });
    const data = fs.readFileSync(get_out_path(session_id));
    await minioClient.putObject('jsonblobs', `${session_id}.json`, data, data.length);
}

async function mark_ingested(session_id: string): Promise<void> {
    const response = await axios.post(`${INGEST_URL}?api_key=${KEY}&session_id=${session_id}`, {
        session_id,
    });
    if (response.status !== 201) {
        throw new Error(`Failed to mark job as ingested: ${response.status} - ${response.statusText}\n${response.data}`);
    }
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
