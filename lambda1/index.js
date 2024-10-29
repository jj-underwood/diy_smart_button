const { ChartJSNodeCanvas } = require('chartjs-node-canvas');
const { S3Client, PutObjectCommand, ListBucketsCommand } = require('@aws-sdk/client-s3');
const { DynamoDBClient, QueryCommand } = require('@aws-sdk/client-dynamodb');
const { unmarshall } = require('@aws-sdk/util-dynamodb');
const moment = require('moment-timezone');
const fs = require('fs');

exports.handler = async (event) => {
  console.log('TZ: ', process.env.TZ);
  console.log('REGION: ', process.env.REGION);
  console.log('IMAGE_WIDTH: ', process.env.IMAGE_WIDTH);
  console.log('IMAGE_HEIGHT: ', process.env.IMAGE_HEIGHT);
  console.log('FONTCONFIG_PATH: ', process.env.FONTCONFIG_PATH);
  console.log('FONTCONFIG_FILE: ', process.env.FONTCONFIG_FILE);
  console.log('FONTCONFIG_CACHE: ', process.env.FONTCONFIG_CACHE);
  console.log('FONTS: ', process.env.FONTS);
  console.log('Fonts directory contents: ', fs.readdirSync('/opt/fonts'));
  console.log('BUCKET: ', process.env.BUCKET);
  console.log('DEVICES: ', process.env.DEVICES);
  console.log('METRICS: ', process.env.METRICS);

  const config = JSON.parse(fs.readFileSync('config.json', 'utf8'));
  const allMetrics = process.env.METRICS.split(',');
  const allDevices = process.env.DEVICES.split(',');
  
  const s3Client = new S3Client({ region: process.env.REGION });
  const dynamoDBClient = new DynamoDBClient({ region: process.env.REGION });

  const endTime = moment().tz(process.env.TZ).set({hour: 6, minute: 0, second: 0}).utc();
  const startTime = endTime.clone().subtract(config.periods.device.main);
  const periods = [{ start: startTime, end: endTime, type: 'main'}];
  config.periods.device.subs.forEach(item => {
    periods.push({ start: periods[0].start.clone().subtract(item), end: periods[0].end.clone().subtract(item), type: "device" });
  })

  const promises = periods.map((period, index) => fetchData(dynamoDBClient, allMetrics, allDevices, period, index));
  const data = await Promise.all(promises);
  const processedData = processData(data, config.periods);
  const images = await createCharts(config, allMetrics, allDevices, parseInt(process.env.IMAGE_WIDTH), parseInt(process.env.IMAGE_HEIGHT), processedData);
  const stored = await storeImages(s3Client, process.env.BUCKET, images);
  return;
}

async function fetchData(dynamoDBClient, metrics, devices, period, periodIndex) {
  let results = [];

  for (let date = period.start.clone(); date.isBefore(period.end) || date.isSame(period.end, 'day'); date.add(1, 'days')) {
    const pk = date.format('YYYY-MM-DD');
    let startSk, endSk;

    if (date.isSame(period.start, 'day')) {
      startSk = `${period.start.format('HH:mm:ss')}`;
      endSk = '23:59:59';
    } else if (date.isSame(period.end, 'day')) {
      startSk = '00:00:00';
      endSk = `${period.end.format('HH:mm:ss')}`;
    } else {
      startSk = '00:00:00';
      endSk = '23:59:59';
    }

    const params = {
      TableName: 'environment-20241007',
      KeyConditionExpression: '#pk = :pk AND #sk BETWEEN :start_sk AND :end_sk',
      ExpressionAttributeNames: {
        '#pk': 'pk',
        '#sk': 'sk'
      },
      ExpressionAttributeValues: {
        ':pk': { S: pk },
        ':start_sk': { S: startSk },
        ':end_sk': { S: endSk }
      }
    };

    try {
      const data = await dynamoDBClient.send(new QueryCommand(params));
      const items = data.Items.map(item => unmarshall(item));
      console.log('QueryCommand success: ', items.length);
      const filteredItems = filter_data(items, devices, metrics);
      console.log('filter_data: ', filteredItems.length);
      results = results.concat(filteredItems);
    } catch (error) {
      console.log('QueryCommand failed: ', error);
    }
  }
  console.log('fetchData: ', results.length);
  return { periodIndex: periodIndex, period: period, data: { data: results } };
}

function filter_data(items, devices, metrics) {
  let filteredItems = [];
  for (let item of items) {
    const skTime = item['sk'].split('#')[0];
    const skDevice = item['sk'].split('#')[1];
    if (devices.includes(skDevice)) {
      let filteredItem = Object.fromEntries(
        Object.entries(item['payload']).filter(([key, value]) => metrics.includes(key))
      );
      filteredItem['time'] = `${item['pk']} ${skTime}`;
      filteredItem['device'] = skDevice;
      filteredItems.push(filteredItem);
    }
  }
  return filteredItems;
}

function processData(dataArray, timePeriods) {
  dataArray.sort((a, b) => a.periodIndex - b.periodIndex);
  const processedData = {};
  const deviceNames = [];
  const metricsNames = [];
  const allTimes = new Set();
  const presenceTimes = new Set();
  
  dataArray.forEach(({ periodIndex, period, data }) => {
    console.log('periodIndex', periodIndex, 'period', period);
    const devNames = [];
    const metNames = [];
    const aTimes = new Set();
    const pTimes = new Set();
    
    if (!data) return;
    const rows = data.data;
    rows.forEach(row => {
      const time = new Date(row.time + "Z");
      time.setMilliseconds(0);
      let roundedTime;
      if (period.type === 'device') {
        const momentTime = moment(time);
        momentTime.add(timePeriods.device.subs[periodIndex - 1]);
        roundedTime = momentTime.toISOString();
      } else {
        roundedTime = time.toISOString();
      }
      let deviceName;
      if (period.type === 'device') {
        deviceName = row.device + "_" + periodIndex;
      } else {
        deviceName = row.device;
      }
      if (!processedData[deviceName]) {
        processedData[deviceName] = { device: row.device };
      }
      if (period.type === 'metric') {
        if (!devNames.includes(deviceName)) {
          devNames.push(deviceName);
        }
      } else {
        if (!deviceNames.includes(deviceName)) {
          deviceNames.push(deviceName);
        }
      }
      for (const [key, value] of Object.entries(row)) {
        if (key === 'time' || key === 'device') continue;
        let metricsName;
        if (period.type === 'metric') {
          metricsName = key + "_" + periodIndex;
        } else {
          metricsName = key;
        }
        if (period.type === 'metric') {
          if (!metNames.includes(metricsName)) {
            metNames.push(metricsName);
          }
        } else {
          if (!metricsNames.includes(metricsName)) {
            metricsNames.push(metricsName);
          }
        }
        if (value === null) continue;
        if (key === 'presence') {
          if (!processedData[deviceName][metricsName]) {
            processedData[deviceName][metricsName] = { value: [], type: key, period: period, periodIndex: periodIndex };
          }
          const presenceValue = value;
          const lastEntry = processedData[deviceName][metricsName].value[processedData[deviceName][metricsName].value.length - 1];
          if (lastEntry) {
            if (lastEntry.presence != presenceValue) {
              lastEntry.end = roundedTime;
              processedData[deviceName][metricsName].value.push({ start: roundedTime, end: roundedTime, presence: presenceValue });
            }
          } else {
            processedData[deviceName][metricsName].value.push({ start: roundedTime, end: roundedTime, presence: presenceValue });
          }
          if (period.type === 'metric') {
            pTimes.add(roundedTime);
          } else {
            presenceTimes.add(roundedTime);
          }
        } else {
          if (!processedData[deviceName][metricsName]) {
            processedData[deviceName][metricsName] = { time: [], value: [], type: key, period: period, periodIndex: periodIndex };
          }
          processedData[deviceName][metricsName].time.push(roundedTime);
          processedData[deviceName][metricsName].value.push(value);
          if (period.type === 'metric') {
            aTimes.add(roundedTime);
          } else {
            allTimes.add(roundedTime);
          }
        }
      }
    });
    if (period.type === 'metric') {
      const aTimesSorted = Array.from(aTimes).sort();
      const pTimesSorted = Array.from(pTimes).sort();
      for (const device of devNames) {
        for (const metrics of metNames) {
          if (!(metrics in processedData[device])) continue;
          if (metrics.startsWith('presence')) {
            const lastSegment = processedData[device][metrics].value[processedData[device][metrics].value.length - 1];
            const lastTime = pTimesSorted[pTimesSorted.length - 1];
            if (new Date(lastSegment.end) < new Date(lastTime)) {
              processedData[device][metrics].value.push({
                start: lastSegment.end,
                end: lastTime,
                presence: "off"
              })
            }
          } else {
            const timeMap = processedData[device][metrics].time.reduce((map, time, index) => {
              map[time] = processedData[device][metrics].value[index];
              return map;
            }, {});
            processedData[device][metrics] = {
              time: aTimesSorted,
              value: aTimesSorted.map(time => timeMap[time] ?? null),
              type: processedData[device][metrics].type,
              period: processedData[device][metrics].period,
              periodIndex: processedData[device][metrics].periodIndex
            };
          }
        }
      }
    }
  });
  if (allTimes.size === 0 && presenceTimes.size === 0) {
    return processedData;
  }
  
  const allTimesSorted = Array.from(allTimes).sort();
  const presenceTimesSorted = Array.from(presenceTimes).sort();
  
  for (const device of deviceNames) {
    for (const metric of metricsNames) {
      if (!(metric in processedData[device])) continue;
      if (metric.startsWith('presence')) {
        const lastSegment = processedData[device][metric].value[processedData[device][metric].value.length - 1];
        const lastTime = presenceTimesSorted[presenceTimesSorted.length - 1];
        if (new Date(lastSegment.end) < new Date(lastTime)) {
          processedData[device][metric].value.push({
            start: lastSegment.end,
            end: lastTime,
            presence: "off"
          })
        }
      } else {
        const timeMap = processedData[device][metric].time.reduce((map, time, index) => {
          map[time] = processedData[device][metric].value[index];
          return map;
        }, {});
        processedData[device][metric] = {
          time: allTimesSorted,
          value: allTimesSorted.map(time => timeMap[time] ?? null),
          type: processedData[device][metric].type,
          period: processedData[device][metric].period,
          periodIndex: processedData[device][metric].periodIndex
        };
      }
    }
  }
  return processedData;
}

async function createCharts(config, allMetrics, allDevices, width, height, data) {
  const chartParams = { 'metrics': {}, 'devices': {} };
  const metricsSet = new Set();
  allMetrics.forEach(metric => {
    const configParams = config.metrics.find(x => x.name === metric);
    for (const device in data) {
      for (const met in data[device]) {
        if (data[device][met].type === metric) {
          metricsSet.add(met);
          if (!chartParams.metrics[met]){
            chartParams.metrics[met] = Object.assign({}, configParams.parameters);
          }
          if (met === metric) {
            chartParams.metrics[met].main = true;
          } else {
            chartParams.metrics[met].main = false;
          }
        }
      }
    }
  });
  const metrics = Array.from(metricsSet);

  let images = [];
  for (metric of metrics) {
    const devices = [];
    allDevices.forEach(device => {
      const configParams = config.devices.find(x => x.name === device);
      for (const dev in data) {
        if (metric in data[dev]) {
          if (data[dev]['device'] === device) {
            devices.push(dev);
            if (!chartParams.devices[dev]) {
              chartParams.devices[dev] = Object.assign({}, configParams.parameters);
            }
            if (dev === device) {
              chartParams.devices[dev].main = true;
            } else {
              chartParams.devices[dev].main = false;
            }
          }
        }
      }
    });
    if (devices.length === 0) {
      continue;
    }

    let image;
    if (metric === 'presence') {
      image = await drawPresenceChart(chartParams, width, height, data, metric, devices);
    } else {
      image = await drawStepLineChart(chartParams, width, height, data, metric, devices);
    }
    if (image != null) {
      images.push(image);
    }
  }

  return images;
}

async function drawStepLineChart(config, width, height, data, metric, devices) {
  const chartJSNodeCanvas = new ChartJSNodeCanvas({ width: width, height: height, backgroundColour: 'white', 
    plugins: { modern: ['chartjs-adapter-moment'] }
   });
  const labels = data[devices[0]][metric]['time'].map((x) => new Date(x));
  
  const start = moment(data[devices[0]][metric].period.start);
  const startD = start.format('YYYY/MM/DD');
  const end = moment(data[devices[0]][metric].period.end);
  const endD = end.format('YYYY/MM/DD');

  let chartTitle;
  if (config.metrics[metric].main) {
    chartTitle = config.metrics[metric].displayName;
  } else {
    if (startD === endD) {
      chartTitle = `${config.metrics[metric].displayName} (${startD})`;
    } else {
      chartTitle = `${config.metrics[metric].displayName} (${startD} - ${endD})`;
    }
  }
  
  let yTitle;
  if ('unit' in config.metrics[metric]) {
    yTitle = config.metrics[metric].unit;
  }
  
  let deviceName;
  const datasets = devices.map(device => {
    if (!data[device] || !data[device][metric]) {
      console.log('no line');
      return null;
    }
    const values = labels.map(label => {
      const idx = data[device][metric]['time'].indexOf(label.toISOString());
      return idx !== -1 ? parseFloat(data[device][metric]['value'][idx]) : null;
    });
    const hasData = values.some(value => value !== null);
    if (!hasData) console.log('no value');
    
    const start = moment(data[device][metric].period.start);
    const startD = start.format('YYYY/MM/DD');
    const end = moment(data[device][metric].period.end);
    const endD = start.format('YYYY/MM/DD');
    let deviceLabel;
    if (config.devices[device].main) {
      deviceName = device;
      deviceLabel = config.devices[device].displayName;
    } else {
      if (startD === endD) {
        deviceLabel = `${config.devices[device].displayName} (${startD})`;
      } else {
        deviceLabel = `${config.devices[device].displayName} (${startD} - ${endD})`;
      }
    }
    
    if (hasData) {
      return {
        label: deviceLabel,
        data: values,
        borderColor: hexToRgba(config.metrics[metric].color, 0.4 * data[device][metric].periodIndex, 1),
        borderWidth: 1.5,
        fill: true,
        backgroundColor: hexToRgba(config.metrics[metric].color, 0.4 * data[device][metric].periodIndex, 0.2),
        stepped: true,
        pointRadius: 0,
        spanGaps: true
      };
    } else {
      return null;
    }
  }).filter(dataset => dataset !== null);

  const configuration = {
    type: 'line',
    data: {
      labels: labels,
      datasets: datasets
    },
    options: {
      scales: {
        x: {
          type: 'time',
          time: {
            unit: 'minute'
          },
          ticks: {
            maxTicksLimit: 10
          }
        },
        y: {
          title: {
            display: config.metrics[metric].unit ? true : false,
            text: `(${config.metrics[metric].unit})`
          }
        }
      },
      plugins: {
        title: {
          display: config.metrics[metric].displayName ? true : false,
          text: chartTitle,
          font: {
            size: 18,
            weight: 'normal'
          },
          padding: 20
        },
        legend: {
          display: true,
          position: 'top',
          labels: {
            usePointStyle: true,
            pointStyle: 'line'
          }
        },
        tooltip: {
          enabled: true,
          mode: 'index',
          intersect: false
        }
      },
      elements: {
        line: {
          tension: 0.4
        }
      }
    }
  };

  const imageName = `${deviceName}_${metric}_${start.format('YYYYMMDD')}-${end.format('YYYYMMDD')}`;

  try {
    const imageBuffer = await chartJSNodeCanvas.renderToBuffer(configuration);
    console.log('renderToBuffer: ', imageName);
    return { imageName: imageName, imageBuffer: imageBuffer };
  } catch (error) {
    console.error('renderToBuffer error', error);
    return null;
  }
}

async function drawPresenceChart(config, width, height, data, metric, devices) {  
  const chartJSNodeCanvas = new ChartJSNodeCanvas({ width: width, height: height, backgroundColour: 'white', 
    plugins: { modern: ['chartjs-adapter-moment'] }
   });
  const start = moment(data[devices[0]][metric].period.start);
  const startD = start.format('YYYY/MM/DD');
  const end = moment(data[devices[0]][metric].period.end);
  const endD = end.format('YYYY/MM/DD');
  
  const imageName = `${metric}_${start.format('YYYYMMDD')}-${end.format('YYYYMMDD')}`;

  let chartTitle;
  if (config.metrics[metric].main) {
    chartTitle = config.metrics[metric].displayName;
  } else {
    if (startD === endD) {
      chartTitle = `${config.metrics[metric].displayName} (${startD})`;
    } else {
      chartTitle = `${config.metrics[metric].displayName} (${startD} - ${endD})`;
    }
  }
  
  const datasets = [];
  const deviceLabels = [];
  devices.forEach(device => {
    const start = moment(data[device][metric].period.start);
    const startD = start.format('YYYY/MM/DD');
    const end = moment(data[device][metric].period.end);
    const endD = start.format('YYYY/MM/DD');
    let deviceLabel;
    if (config.devices[device].main) {
      deviceLabel = config.devices[device].displayName;
    } else {
      if (startD === endD) {
        deviceLabel = `${config.devices[device].displayName} (${startD})`;
      } else {
        deviceLabel = `${config.devices[device].displayName} (${startD} - ${endD})`;
      }
    }
    deviceLabels.push(deviceLabel);
    const segments = data[device][metric].value;
    segments.forEach(segment => {
      datasets.push({
        label: `${deviceLabel}`,
        data: [{
          x: [new Date(segment.start), new Date(segment.end)],
          y: deviceLabel
        }],
        backgroundColor: segment.presence === 'on' ? 'orange' : 'gray'
      });
    });
  });
  
  const configuration = {
    type: 'bar',
    data: {
      datasets: datasets
    },
    options: {
      indexAxis: 'y',
      scales: {
        x: {
          type: 'time',
          time: {
            unit: 'minute'
          },
          ticks: {
            maxTicksLimit: 10
          },
        },
        y: {
          stacked: true,
          type: 'category',
          labels: deviceLabels
        }
      },
      plugins: {
        title: {
          display: true,
          text: chartTitle,
          font: {
            size: 18,
            weight: 'normal'
          },
          padding: 20
        },
        legend: {
          display: false
        },
        tooltip: {
          callbacks: {
            label: function(context) {
              const label = context.dataset.label || '';
            const start = new Date(context.raw.x[0]).toLocaleString([], { month: 'short', day: 'numeric', year: 'numeric', hour: '2-digit', minute: '2-digit' });
            const end = new Date(context.raw.x[1]).toLocaleString([], { month: 'short', day: 'numeric', year: 'numeric', hour: '2-digit', minute: '2-digit' });
              return `${label}: ${start} - ${end}`;
            }
          }
        }
      }
    }
  };

  try {
    const imageBuffer = await chartJSNodeCanvas.renderToBuffer(configuration);
    console.log('renderToBuffer: ', imageName);
    return { imageName: imageName, imageBuffer: imageBuffer };
  } catch (error) {
    console.error('renderToBufferError', error);
    return null;
  }
}

function getRandomColor() {
  const letters = '0123456789ABCDEF';
  let color = '#';
  for (let i = 0; i < 6; i++) {
    color += letters[Math.floor(Math.random() * 16)];
  }
  return color;
}

function hexToRgba(hex, percent=0, alpha=1) {
  hex = hex.replace('#', '');

  if (!/^([0-9A-Fa-f]{3}){1,2}$/.test(hex)) {
    throw new Error('Invalid hex color code');
  }

  if (hex.length === 3) {
    hex = hex.split('').map(char => char + char).join('');
  }

  let r = parseInt(hex.slice(0, 2), 16);
  let g = parseInt(hex.slice(2, 4), 16);
  let b = parseInt(hex.slice(4, 6), 16);

  r = Math.min(Math.round(r + (255 - r) * percent), 255);
  g = Math.min(Math.round(g + (255 - g) * percent), 255);
  b = Math.min(Math.round(b + (255 - b) * percent), 255);
  
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

async function storeImages(s3Client, bucketName, images) {
  keys = [];
  for (image of images) {
    if (!image) {
      continue;
    }
    const key = `${image.imageName}.png`;
    const s3params = {
      Bucket: bucketName,
      Key: key,
      Body: image.imageBuffer,
      ContentType: 'image/png'
    };
    try {
      const results = await s3Client.send(new PutObjectCommand(s3params));
      console.log('PutObject: ', results);
      keys.push(key);
    } catch (error) {
      console.error('PutObjectError', error);
    }
  }
  return keys;
}
