/*
* MIT License
* 
* Copyright (c) 2017 Philip Tibom, Jonathan Jansson, Rickard Laurenius, 
* Tobias Alldén, Martin Chemander, Sherry Davar
* 
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
* 
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
* 
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
*
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
*
* This module was originally included in the Unity3D Lidar Simulation by Philip Tibom et al.
* It was modified to fit the needs of Movement and Collision Prediction System by Karol Oleszek.
*/

using UnityEngine;
using UnityEngine.Networking;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;

/// <summary>
/// The data structure in which to save the lidar data.
///  @author: Tobias Alldén
/// </summary>
public class LidarStorage : MonoBehaviour {

    public delegate void Filled();
    public static event Filled HaveData;
	private Dictionary<float, List<LinkedList<SphericalCoordinate>>> dataStorage;
    private Queue<SphericalCoordinate> buffer;

	public LidarStorage()
	{
		this.dataStorage = new Dictionary<float, List<LinkedList<SphericalCoordinate>>>();
        this.buffer = new Queue<SphericalCoordinate>();
        LidarSensor.OnScanned += Save;
	}

    void OnDestroy()
    {
        LidarSensor.OnScanned -= Save;
    }
    

    /// <summary>
    /// Saves the current collected points on the given timestamp. 
    /// </summary>
    /// <param name="newTime"></param>
    public void Save(float time, LinkedList<SphericalCoordinate> hits)
	{
        if (hits.Count != 0) {
            int maxBufferSize = 5000;
            lock(buffer) {
                foreach(SphericalCoordinate s in hits) {
                    buffer.Enqueue(s);
                }
                if(buffer.Count >= maxBufferSize) {
                    string payload = "[";
                    List<string> toSend = new List<string>();
                    while(buffer.Count > 0){
                        SphericalCoordinate s = buffer.Dequeue();
                        toSend.Add(s.ToCartesian().ToString());
                    }
                    payload += string.Join(",", toSend);
                    payload += "]";
                    WWWForm form = new WWWForm();
                    form.AddField("payload", payload);
                    UnityWebRequest www = UnityWebRequest.Post("http://localhost:8000/", form);
                    www.SendWebRequest();
                    if (www.result != UnityWebRequest.Result.Success)
                    {
                        Debug.Log(www.error);
                    }
                    else
                    {
                        Debug.Log("Form upload complete!");
                    }
                }
            }

            if (!dataStorage.ContainsKey(time))
            {
                    
                List<LinkedList<SphericalCoordinate>> keyList = new List<LinkedList<SphericalCoordinate>>();
                keyList.Add(hits);
                dataStorage.Add(time, keyList);
            } else
            {
                dataStorage[time].Add(hits);
            }
        }		
	}


    public Dictionary<float, List<LinkedList<SphericalCoordinate>>> GetData()
    {
        return dataStorage;
    }

    public void SetData(Dictionary<float,List<LinkedList<SphericalCoordinate>>> data )
    {
        this.dataStorage = data;
        if(HaveData != null && data != null)
        {
            HaveData();
        }
    }
 


}
