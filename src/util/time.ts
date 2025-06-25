export let TIMEZONE: { timeZone: string };
export function readTimezone(note) {
    const tz = process.env.TIMEZONE;
    if (!tz) {
        return {};
    }
    console.log(note, "reading timezone", tz);
    TIMEZONE = {timeZone: tz as string};
}